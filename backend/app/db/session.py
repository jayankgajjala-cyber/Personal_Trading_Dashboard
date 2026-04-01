from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase
from app.core.config import settings

# Standardize the Driver Prefix and strip URL parameters
db_url = settings.DATABASE_URL
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+asyncpg://", 1)
elif db_url.startswith("postgresql://") and not db_url.startswith("postgresql+asyncpg://"):
    db_url = db_url.replace("postgresql://", "postgresql+asyncpg://", 1)

# We strip the '?' and everything after it to avoid conflicts with connect_args
db_url = db_url.split("?")[0]

engine = create_async_engine(
    db_url,
    # ... other settings
    connect_args={
        "statement_cache_size": 0,
        "prepared_statement_cache_size": 0,
        # Using a named function is often more stable than a lambda 
        # during SQLAlchemy's internal dialect initialization.
        "prepared_statement_name_func": lambda name=None: "",
    },
)

AsyncSessionLocal = async_sessionmaker(
    engine, class_=AsyncSession, expire_on_commit=False
)

class Base(DeclarativeBase):
    pass

async def get_db():
    async with AsyncSessionLocal() as session:
        try:
            yield session
        finally:
            await session.close()