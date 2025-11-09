import re

from pydantic import BaseModel, Field, field_validator

_NAME_PATTERN = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_DATA_KEY_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")


def normalize_secret_name(value: str) -> str:
    candidate = value.strip()
    if not _NAME_PATTERN.fullmatch(candidate):
        raise ValueError(
            "Secret name must start/end with an alphanumeric character and "
            "contain only lowercase letters, numbers, and dashes"
        )
    return candidate


class SecretCreate(BaseModel):
    name: str = Field(
        ...,
        min_length=1,
        max_length=253,
        description="DNS-1123 compatible secret name",
    )
    data: dict[str, str] = Field(
        ...,
        min_length=1,
        description="Key/value pairs to persist in the Kubernetes secret",
    )

    @field_validator("name")
    @classmethod
    def validate_name(cls, value: str) -> str:
        return normalize_secret_name(value)

    @field_validator("data")
    @classmethod
    def validate_data(cls, value: dict[str, str]) -> dict[str, str]:
        if not value:
            raise ValueError("Secret data must include at least one entry")

        for key, secret_value in value.items():
            if not _DATA_KEY_PATTERN.fullmatch(key):
                raise ValueError(
                    "Secret keys may only include letters, numbers, dot, dash, or underscore"
                )
            if secret_value is None:
                raise ValueError("Secret values cannot be null")
        return value


class SecretRef(BaseModel):
    name: str


class SecretDetail(SecretRef):
    keys: list[str] = Field(default_factory=list)
