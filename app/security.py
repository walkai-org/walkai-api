import hashlib
import os
import secrets
from datetime import datetime, timedelta, timezone

import jwt
from argon2 import PasswordHasher
from dotenv import load_dotenv

load_dotenv()

ph = PasswordHasher(time_cost=3, memory_cost=64 * 1024, parallelism=2)
JWT_SECRET = os.getenv("JWT_SECRET")
JWT_ALGO = os.getenv("JWT_ALGO")
ACCESS_MIN = int(os.getenv("ACCESS_MIN"))


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
    now = datetime.now(timezone.utc)
    payload = {
        "sub": sub,
        "role": role,
        "iat": int(now.timestamp()),
        "exp": int((now + ttl).timestamp()),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)


def create_access(sub: str, role: str) -> str:
    return create_token(sub, role, timedelta(minutes=ACCESS_MIN))
