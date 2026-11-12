"""Application settings loaded from environment variables."""

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def normalize_postgres_url(url: str) -> str:
    """Normalize a PostgreSQL URL to use the Psycopg 3 SQLAlchemy dialect."""
    stripped = url.strip()
    if stripped.startswith("postgres://"):
        return "postgresql+psycopg://" + stripped.removeprefix("postgres://")
    if stripped.startswith("postgresql://"):
        return "postgresql+psycopg://" + stripped.removeprefix("postgresql://")
    if stripped.startswith("postgresql+psycopg://"):
        return stripped
    raise ValueError(
        "Only PostgreSQL URLs using psycopg (postgresql+psycopg://) are supported."
    )


class Settings(BaseSettings):
    """Environment-backed runtime and database configuration."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str | None = Field(default=None, alias="DATABASE_URL")
    test_database_url: str | None = Field(default=None, alias="TEST_DATABASE_URL")
    sqlalchemy_echo: bool = Field(default=False, alias="SQLALCHEMY_ECHO")
    db_pool_size: int = Field(default=5, alias="DB_POOL_SIZE")
    db_max_overflow: int = Field(default=10, alias="DB_MAX_OVERFLOW")
    worker_poll_interval_seconds: float = Field(
        default=0.5, gt=0.0, alias="WORKER_POLL_INTERVAL_SECONDS"
    )
    worker_lease_ttl_seconds: float = Field(
        default=30.0, gt=0.0, alias="WORKER_LEASE_TTL_SECONDS"
    )
    worker_heartbeat_interval_seconds: float = Field(
        default=10.0, gt=0.0, alias="WORKER_HEARTBEAT_INTERVAL_SECONDS"
    )
    worker_recovery_interval_seconds: float = Field(
        default=5.0, gt=0.0, alias="WORKER_RECOVERY_INTERVAL_SECONDS"
    )
    worker_recovery_batch_size: int = Field(
        default=50, ge=1, alias="WORKER_RECOVERY_BATCH_SIZE"
    )

    def require_database_url(self) -> str:
        """Return normalized DATABASE_URL or raise if unconfigured."""
        if not self.database_url:
            raise RuntimeError(
                "DATABASE_URL environment variable is required and must not be empty."
            )
        return normalize_postgres_url(self.database_url)

    def get_test_database_url(self) -> str | None:
        """Return normalized TEST_DATABASE_URL (or fallback DATABASE_URL) if set."""
        raw_url = self.test_database_url or self.database_url
        if not raw_url:
            return None
        return normalize_postgres_url(raw_url)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return cached application settings loaded from environment."""
    return Settings()
