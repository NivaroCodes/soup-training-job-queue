"""Synchronous SQLAlchemy 2.x + Psycopg 3 engine and session management."""

from collections.abc import Iterator
from contextlib import contextmanager
from functools import lru_cache

from sqlalchemy import Engine, create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import get_settings, normalize_postgres_url


def create_db_engine(database_url: str | None = None) -> Engine:
    """Create a synchronous SQLAlchemy engine configured for PostgreSQL + Psycopg 3."""
    settings = get_settings()
    resolved_url = (
        normalize_postgres_url(database_url)
        if database_url is not None
        else settings.require_database_url()
    )
    return create_engine(
        resolved_url,
        echo=settings.sqlalchemy_echo,
        pool_pre_ping=True,
        pool_size=settings.db_pool_size,
        max_overflow=settings.db_max_overflow,
    )


def create_session_factory(engine: Engine) -> sessionmaker[Session]:
    """Create a synchronous session factory bound to the given engine."""
    return sessionmaker(
        bind=engine,
        autoflush=False,
        expire_on_commit=False,
        class_=Session,
    )


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    """Return the process-wide cached SQLAlchemy engine."""
    return create_db_engine()


@lru_cache(maxsize=1)
def get_session_factory() -> sessionmaker[Session]:
    """Return the process-wide cached session factory."""
    return create_session_factory(get_engine())


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional database session scope with rollback on error."""
    session = get_session_factory()()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
