"""Alembic migration environment configured for synchronous SQLAlchemy 2.x + Psycopg 3."""

from __future__ import annotations

from logging.config import fileConfig

from alembic import context
from sqlalchemy import engine_from_config, pool

from app.core.config import get_settings, normalize_postgres_url
from app.db import Base

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

target_metadata = Base.metadata


def _resolve_online_url() -> str:
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        return normalize_postgres_url(configured)
    return get_settings().require_database_url()


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode using PostgreSQL dialect DDL generation."""
    configured = config.get_main_option("sqlalchemy.url")
    if configured:
        url = normalize_postgres_url(configured)
    else:
        settings = get_settings()
        url = (
            normalize_postgres_url(settings.database_url)
            if settings.database_url
            else None
        )

    if url is not None:
        context.configure(
            url=url,
            target_metadata=target_metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
            compare_type=True,
        )
    else:
        context.configure(
            dialect_name="postgresql",
            target_metadata=target_metadata,
            literal_binds=True,
            dialect_opts={"paramstyle": "named"},
            compare_type=True,
        )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode against a live PostgreSQL connection."""
    connectable = config.attributes.get("connection")
    if connectable is not None:
        context.configure(
            connection=connectable,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()
        return

    section = config.get_section(config.config_ini_section, {})
    section["sqlalchemy.url"] = _resolve_online_url()

    engine = engine_from_config(
        section,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )

    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
        )
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
