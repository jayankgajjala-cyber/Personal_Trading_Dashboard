from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # JWT
    SECRET_KEY:                   str
    ALGORITHM:                    str = "HS256"
    ACCESS_TOKEN_EXPIRE_MINUTES:  int = 60

    # Resend (OTP email)
    RESEND_API_KEY: str
    OTP_FROM_EMAIL: str

    # Supabase / DB
    DATABASE_URL:      str
    SUPABASE_URL:      str
    SUPABASE_ANON_KEY: str

    # MongoDB (News Engine)
    MONGODB_URI:  str = "mongodb://localhost:27017"

    # External API keys
    FINNHUB_API_KEY: str = ""
    GNEWS_API_KEY:   str = ""

    # CORS
    ALLOWED_ORIGINS: str = "http://localhost:3000"

    @property
    def origins_list(self) -> List[str]:
        return [o.strip().rstrip("/") for o in self.ALLOWED_ORIGINS.split(",") if o.strip()]

    class Config:
        env_file = ".env"
        extra    = "ignore"


settings = Settings()
