"""FastAPI dependencies."""

from collections.abc import Iterator

from sqlalchemy.orm import Session

from app.db.session import get_session_factory


def get_db() -> Iterator[Session]:
    """Yield a synchronous PostgreSQL SQLAlchemy session per request."""
    session = get_session_factory()()
    try:
        yield session
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
