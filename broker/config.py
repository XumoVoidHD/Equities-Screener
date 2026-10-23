from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from dotenv import load_dotenv

# Automatically load .env from project root
load_dotenv(override=False)


@dataclass
class DhanConfig:
    """Configuration container for Dhan credentials and settings."""
    client_id: str
    access_token: str | None = None
    pin: str | None = None
    totp_secret: str | None = None
    token_cache_path: Path = Path("data/dhan/token.json")

    @classmethod
    def from_env(cls, env_path: str | Path | None = None) -> DhanConfig:
        """Load configuration from environment variables or .env file."""
        if env_path:
            load_dotenv(dotenv_path=env_path, override=True)

        client_id = os.getenv("DHAN_CLIENT_ID") or os.getenv("DHANHQ_CLIENT_ID") or ""
        access_token = os.getenv("DHAN_ACCESS_TOKEN") or os.getenv("DHANHQ_ACCESS_TOKEN") or None
        pin = os.getenv("DHAN_PIN") or None
        totp_secret = os.getenv("DHAN_TOTP_SECRET") or os.getenv("DHAN_TOTP") or None
        cache_path_str = os.getenv("DHAN_TOKEN_CACHE_PATH", "data/dhan/token.json")

        if not client_id:
            raise ValueError(
                "Missing DHAN_CLIENT_ID in environment or .env file. "
                "Please configure DHAN_CLIENT_ID."
            )

        return cls(
            client_id=client_id.strip(),
            access_token=access_token.strip() if access_token else None,
            pin=pin.strip() if pin else None,
            totp_secret=totp_secret.replace(" ", "").strip() if totp_secret else None,
            token_cache_path=Path(cache_path_str),
        )
