import hashlib
import secrets

from argon2 import PasswordHasher

ph = PasswordHasher(time_cost=3, memory_cost=64 * 1024, parallelism=2)


def generate_raw_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)


def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()


def hash_password(plain: str) -> str:
    return ph.hash(plain)
