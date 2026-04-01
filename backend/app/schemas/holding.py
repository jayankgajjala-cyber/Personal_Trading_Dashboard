from pydantic import BaseModel, Field, UUID4
from typing import Optional
from datetime import datetime


# ── Auth ────────────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str


class OTPVerifyRequest(BaseModel):
    username: str
    otp: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"


# ── Holdings ─────────────────────────────────────────────────────────────────

class HoldingCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=20)
    stock_name: str
    quantity: float = Field(..., gt=0)
    average_buy_price: float = Field(..., gt=0)
    exchange: str = Field(default="NSE", description="NSE | BSE | US | CRYPTO")


class HoldingUpdate(BaseModel):
    additional_quantity: float = Field(..., gt=0)
    buy_price: float = Field(..., gt=0)


class HoldingSellRequest(BaseModel):
    sell_quantity: float = Field(..., gt=0)
    sell_price: float = Field(..., gt=0)


class HoldingResponse(BaseModel):
    id: UUID4
    symbol: str
    stock_name: str
    quantity: float
    average_buy_price: float
    invested_amount: float
    exchange: str
    created_at: datetime
    updated_at: datetime

    class Config:
        from_attributes = True


class HoldingWithLTP(HoldingResponse):
    """Runtime-enriched response — computed in memory, never stored."""
    ltp: Optional[float] = None
    current_value: Optional[float] = None
    pnl: Optional[float] = None
    pnl_percent: Optional[float] = None
    day_change: Optional[float] = None
    signal: Optional[str] = None
