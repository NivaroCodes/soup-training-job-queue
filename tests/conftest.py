"""Pytest fixtures for PostgreSQL schema, Alembic migrations, and SQLAlchemy sessions."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from fastapi.testclient import TestClient
from sqlalchemy import Engine, delete
from sqlalchemy.orm import Session, sessionmaker

from app.api.dependencies import get_db
from app.core.config import get_settings
from app.db import ExecutionEffect, Job, create_db_engine, create_session_factory
from app.main import create_app

ROOT_DIR = Path(__file__).resolve().parent.parent


def _build_alembic_config(database_url: str) -> Config:
    cfg = Config(str(ROOT_DIR / "alembic.ini"))
    cfg.set_main_option("script_location", str(ROOT_DIR / "alembic"))
    cfg.set_main_option("sqlalchemy.url", database_url)
    return cfg


@pytest.fixture(scope="session")
def postgres_url() -> str:
    """Resolve PostgreSQL URL from TEST_DATABASE_URL or DATABASE_URL."""
    url = get_settings().get_test_database_url()
    if not url:
        pytest.fail(
            "TEST_DATABASE_URL or DATABASE_URL must be set to a PostgreSQL database; "
            "SQLite fallback is disallowed."
        )
    return url


@pytest.fixture(scope="session")
def db_engine(postgres_url: str) -> Iterator[Engine]:
    """Run Alembic migrations against PostgreSQL and yield a synchronous Engine."""
    cfg = _build_alembic_config(postgres_url)
    command.downgrade(cfg, "base")
    command.upgrade(cfg, "head")

    engine = create_db_engine(postgres_url)
    try:
        yield engine
    finally:
        engine.dispose()


@pytest.fixture(scope="session")
def session_factory(db_engine: Engine) -> sessionmaker[Session]:
    """Return a session factory bound to the migrated PostgreSQL engine."""
    return create_session_factory(db_engine)


@pytest.fixture()
def db_session(session_factory: sessionmaker[Session]) -> Iterator[Session]:
    """Yield a clean PostgreSQL session and purge test rows after each test."""
    session = session_factory()
    try:
        yield session
    finally:
        session.rollback()
        session.execute(delete(ExecutionEffect))
        session.execute(delete(Job))
        session.commit()
        session.close()


@pytest.fixture()
def alembic_config(postgres_url: str) -> Config:
    """Provide an Alembic Config bound to the PostgreSQL test database."""
    return _build_alembic_config(postgres_url)


@pytest.fixture()
def client(
    session_factory: sessionmaker[Session],
    db_session: Session,
) -> Iterator[TestClient]:
    """Provide a FastAPI TestClient backed by per-request PostgreSQL sessions."""
    application = create_app()

    def _override_get_db() -> Iterator[Session]:
        session = session_factory()
        try:
            yield session
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    application.dependency_overrides[get_db] = _override_get_db
    with TestClient(application) as test_client:
        yield test_client
    application.dependency_overrides.clear()
