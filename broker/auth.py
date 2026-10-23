from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
import pyotp
import dhanhq
from broker.config import DhanConfig

logger = logging.getLogger("dhan.auth")


class DhanAuthManager:
    """Manages authentication with DhanHQ:

    - Resolves access tokens from static config, cache file, or dynamic TOTP minting.
    - Caches valid tokens in `data/dhan/token.json` with expiry metadata.
    - Automatically refreshes tokens when expired (Dhan tokens are valid ~24 hours).
    - Prevents unnecessary API calls to respect Dhan's 2-minute token regeneration limit.
    """

    def __init__(self, config: DhanConfig):
        self.config = config
        self._access_token: str | None = None
        self._dhan_context: dhanhq.DhanContext | None = None

    def get_access_token(self, force_refresh: bool = False) -> str:
        """Retrieve a valid access token."""
        if not force_refresh:
            # 1. Direct configured token
            if self.config.access_token:
                self._access_token = self.config.access_token
                return self._access_token

            # 2. In-memory cached token
            if self._access_token:
                return self._access_token

            # 3. File cached token (checked against expiryTime and file age)
            cached_token = self._read_cached_token()
            if cached_token:
                self._access_token = cached_token
                return self._access_token

        # 4. Mint token via PIN + TOTP
        return self._mint_and_cache_token()

    def is_token_valid(self, token: str) -> bool:
        """Validate token by querying Dhan profile API, suppressing noisy root logs on 401."""
        root_logger = logging.getLogger()
        prev_level = root_logger.level
        try:
            root_logger.setLevel(logging.CRITICAL)
            login = dhanhq.DhanLogin(self.config.client_id)
            res = login.user_profile(token)
            return isinstance(res, dict) and "dhanClientId" in res
        except Exception:
            return False
        finally:
            root_logger.setLevel(prev_level)

    def get_dhan_context(self, force_refresh: bool = False) -> dhanhq.DhanContext:
        """Create or return a configured DhanContext instance."""
        token = self.get_access_token(force_refresh=force_refresh)
        if self._dhan_context is None or force_refresh:
            self._dhan_context = dhanhq.DhanContext(self.config.client_id, token)
        return self._dhan_context

    def _read_cached_token(self) -> str | None:
        cache_file = self.config.token_cache_path
        if not cache_file.exists():
            return None

        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)

            token = data.get("accessToken") or data.get("access_token")
            if not token:
                return None

            # Verify client ID matches
            client_id = data.get("dhanClientId") or data.get("client_id")
            if client_id and str(client_id) != str(self.config.client_id):
                logger.info("Cached token client_id mismatch, will re-mint.")
                return None

            # Check expiration timestamp if available
            expiry_str = data.get("expiryTime") or data.get("expires_at")
            if expiry_str:
                try:
                    # e.g. 2026-10-24T13:38:30.705
                    expiry_dt = datetime.fromisoformat(expiry_str)
                    now_utc = datetime.now(timezone.utc)
                    # If timezone naive, assume local or UTC
                    if expiry_dt.tzinfo is None:
                        now = datetime.now()
                    else:
                        now = now_utc
                    if now >= expiry_dt:
                        logger.info("Cached Dhan token has expired at %s", expiry_str)
                        return None
                except Exception as e:
                    logger.debug("Could not parse expiry date '%s': %s", expiry_str, e)

            # Check file modification age as fallback (23 hours limit)
            mtime = cache_file.stat().st_mtime
            age_seconds = datetime.now().timestamp() - mtime
            if age_seconds > 23 * 3600:
                logger.info("Cached Dhan token file is older than 23 hours.")
                return None

            # Dhan invalidates daily tokens across the 15:30 IST market settlement boundary
            now = datetime.now()
            today_settlement = now.replace(hour=15, minute=30, second=0, microsecond=0).timestamp()
            if mtime < today_settlement <= now.timestamp():
                logger.info("Cached Dhan token was minted prior to today's 15:30 IST market settlement; refreshing.")
                return None

            logger.info("Using cached Dhan access token from %s", cache_file)
            return str(token)

        except Exception as e:
            logger.warning("Failed to read cached token from %s: %s", cache_file, e)
            return None

    def _mint_and_cache_token(self) -> str:
        """Mint a fresh token using PIN and TOTP secret."""
        if not self.config.pin or not self.config.totp_secret:
            raise ValueError(
                "Cannot mint Dhan access token: DHAN_PIN and DHAN_TOTP_SECRET must be set in .env "
                "or DHAN_ACCESS_TOKEN must be provided directly."
            )

        logger.info("Minting new Dhan access token for Client ID: %s", self.config.client_id)
        # Protect against TOTP 30s window boundary expiration
        rem = time.time() % 30
        if rem > 25:
            time.sleep(30 - rem + 1)

        totp_code = pyotp.TOTP(self.config.totp_secret).now()
        login = dhanhq.DhanLogin(self.config.client_id)
        token_res = login.generate_token(self.config.pin, totp_code)

        if not isinstance(token_res, dict) or "accessToken" not in token_res:
            error_msg = token_res.get("message") if isinstance(token_res, dict) else str(token_res)
            # If rate limited, try to fall back to cached token even if old
            if "once every 2 minutes" in str(error_msg).lower():
                cached = self._read_cached_token_ignore_expiry()
                if cached:
                    logger.warning(
                        "Dhan token rate limit hit; falling back to existing cached token."
                    )
                    self._access_token = cached
                    return cached
            raise RuntimeError(f"Dhan token generation failed: {error_msg}")

        access_token = token_res["accessToken"]
        self._access_token = access_token

        # Save to cache
        self._save_token_cache(token_res)
        return access_token

    def _read_cached_token_ignore_expiry(self) -> str | None:
        cache_file = self.config.token_cache_path
        if not cache_file.exists():
            return None
        try:
            with open(cache_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data.get("accessToken") or data.get("access_token")
        except Exception:
            return None

    def _save_token_cache(self, token_data: dict) -> None:
        try:
            cache_file = self.config.token_cache_path
            cache_file.parent.mkdir(parents=True, exist_ok=True)
            with open(cache_file, "w", encoding="utf-8") as f:
                json.dump(token_data, f, indent=2)
            logger.info("Successfully cached new Dhan access token at %s", cache_file)
        except Exception as e:
            logger.warning("Failed to save token to cache %s: %s", self.config.token_cache_path, e)
