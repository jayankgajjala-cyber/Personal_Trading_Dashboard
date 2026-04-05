from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.core.config import settings
from app.db.session import engine, Base
from app.api.routes import auth, holdings
from app.api.routes.global_routes import router as global_router
from app.api.routes.news_routes import router as news_router
from app.services.news_service import start_news_loop, stop_news_loop
import app.models  # noqa: F401 — triggers models/__init__.py, registers all ORM classes with Base


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Create tables on startup (use Alembic for production migrations)
    # The statement_cache_size=0 fix should be applied in app/db/session.py
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    start_news_loop()
    yield
    stop_news_loop()
    await engine.dispose()


app = FastAPI(
    title="Quantedge API",
    version="1.0.0",
    description="Portfolio management backend for Quantedge",
    lifespan=lifespan,
)

# ── CORS ─────────────────────────────────────────────────────────────────────
# Clean origins to remove accidental whitespace from environment variables
clean_origins = [origin.strip().rstrip('/') for origin in settings.origins_list]

app.add_middleware(
    CORSMiddleware,
    allow_origins=clean_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Routers ───────────────────────────────────────────────────────────────────
app.include_router(auth.router)
app.include_router(holdings.router)
app.include_router(global_router)
app.include_router(news_router)


@app.get("/health", tags=["health"])
async def health():
    return {"status": "ok", "service": "quantedge-backend"}