"""FastAPI application entrypoint."""

from fastapi import FastAPI

from app.api.routes.jobs import router as jobs_router


def create_app() -> FastAPI:
    """Create and configure the FastAPI application."""
    application = FastAPI(
        title="Soup Training Job Queue API",
        version="0.1.0",
    )

    @application.get("/health", tags=["health"])
    def health() -> dict[str, str]:
        return {"status": "ok"}

    application.include_router(jobs_router)
    return application


app = create_app()
