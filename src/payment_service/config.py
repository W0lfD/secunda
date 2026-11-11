from urllib.parse import urlsplit

from pydantic import Field, HttpUrl, SecretStr, TypeAdapter, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


def normalize_origin(value: str) -> str:
    validated = TypeAdapter(HttpUrl).validate_python(value)
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid webhook origin") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("webhook origin must contain only scheme, host and optional port")
    host = validated.host
    default_port = 80 if parsed.scheme == "http" else 443
    return f"{parsed.scheme}://{host}" + (
        f":{port}" if port is not None and port != default_port else ""
    )


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="PAYMENT_", env_file=".env", extra="ignore")

    api_key: SecretStr
    webhook_allowed_origins: list[str] = Field(min_length=1)
    database_url: str
    rabbitmq_url: str
    webhook_timeout_seconds: float = Field(default=10, gt=0, le=300)
    gateway_timeout_seconds: float = Field(default=10, gt=0, le=300)
    broker_timeout_seconds: float = Field(default=10, gt=0, le=300)
    database_timeout_seconds: float = Field(default=10, gt=0, le=300)

    @field_validator("api_key")
    @classmethod
    def validate_api_key(cls, value: SecretStr) -> SecretStr:
        if len(value.get_secret_value().strip()) < 32:
            raise ValueError("API key must contain at least 32 non-padding characters")
        return value

    @field_validator("webhook_allowed_origins")
    @classmethod
    def validate_origins(cls, values: list[str]) -> list[str]:
        return [normalize_origin(value) for value in values]
