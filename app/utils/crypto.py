"""Symmetric encryption mirror for the worker.

Identical surface to `backend/app/utils/crypto.py` so behaviour cannot drift.
The shared `CREDENTIAL_ENCRYPTION_KEY` lets the worker open the job payload
the backend sealed on the task row, and the credential envelope inside it.
"""

from __future__ import annotations

import base64

from cryptography.fernet import Fernet, InvalidToken

from app.config import settings


def _build_cipher() -> Fernet:
    key = settings.CREDENTIAL_ENCRYPTION_KEY
    if not key:
        raise RuntimeError(
            "CREDENTIAL_ENCRYPTION_KEY is not set. The worker requires the same "
            "Fernet key as the backend to decrypt credential envelopes."
        )
    try:
        return Fernet(key.encode() if isinstance(key, str) else key)
    except (ValueError, TypeError) as e:
        raise RuntimeError(f"CREDENTIAL_ENCRYPTION_KEY is malformed: {e}") from e


_cipher = _build_cipher()


def encrypt(plaintext: str) -> bytes:
    return _cipher.encrypt(plaintext.encode("utf-8"))


def decrypt(token: bytes) -> str:
    return _cipher.decrypt(token).decode("utf-8")


def encrypt_b64(plaintext: str) -> str:
    return base64.b64encode(encrypt(plaintext)).decode("ascii")


def decrypt_b64(token_b64: str) -> str:
    return decrypt(base64.b64decode(token_b64.encode("ascii")))


__all__ = ["encrypt", "decrypt", "encrypt_b64", "decrypt_b64", "InvalidToken"]
