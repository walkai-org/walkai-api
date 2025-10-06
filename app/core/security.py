import base64
import hashlib
import os
import secrets
from datetime import UTC, datetime, timedelta

import jwt
from argon2 import PasswordHasher

from app.core.config import get_settings

ph = PasswordHasher(time_cost=3, memory_cost=64 * 1024, parallelism=2)
settings = get_settings()
JWT_SECRET = settings.jwt_secret
JWT_ALGO = settings.jwt_algo
ACCESS_MIN = settings.access_min


def generate_raw_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def hash_password(plain: str) -> str:
    return ph.hash(plain)


def verify_password(plain: str, hashed: str) -> bool:
    try:
        return ph.verify(hashed, plain)
    except Exception:
        return False


def create_token(sub: str, role: str, ttl: timedelta) -> str:
    now = datetime.now(UTC)
    payload = {
        "sub": sub,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + ttl).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def create_access(sub: str, role: str) -> str:
    return create_token(sub, role, timedelta(minutes=ACCESS_MIN))


def gen_pkce():
    code_verifier = base64.urlsafe_b64encode(os.urandom(32)).rstrip(b"=").decode()
    challenge = hashlib.sha256(code_verifier.encode()).digest()
    code_challenge = base64.urlsafe_b64encode(challenge).rstrip(b"=").decode()
    return code_verifier, code_challenge
