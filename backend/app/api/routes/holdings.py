import io
import logging
from typing import List

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File, status
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.routes.auth import get_current_user
from app.db.session import get_db
from app.models.holding import Holding
from app.schemas.holding import (
    HoldingCreate,
    HoldingResponse,
    HoldingUpdate,
    HoldingSellRequest,
    HoldingWithLTP,
)
from app.utils.market_data import fetch_ltp_batch

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/holdings", tags=["holdings"])


def calc_invested(qty: float, avg: float) -> float:
    return round(qty * avg, 2)


def _enrich(holding: Holding, ltp_map: dict) -> HoldingWithLTP:
    base = {c.name: getattr(holding, c.name) for c in Holding.__table__.columns}
    ltp = ltp_map.get(holding.symbol)

    current_value: float | None = None
    pnl: float | None = None
    pnl_percent: float | None = None

    if ltp is not None:
        current_value = round(ltp * holding.quantity, 2)
        pnl = round(current_value - holding.invested_amount, 2)
        pnl_percent = round((pnl / holding.invested_amount) * 100, 2) if holding.invested_amount else None

    return HoldingWithLTP(
        **base,
        ltp=ltp,
        current_value=current_value,
        pnl=pnl,
        pnl_percent=pnl_percent,
        signal=None,
    )


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.get("", response_model=List[HoldingWithLTP])
async def list_holdings(
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
):
    result = await db.execute(select(Holding).order_by(Holding.symbol))
    holdings = result.scalars().all()

    if not holdings:
        return []

    symbols   = [h.symbol   for h in holdings]
    exchanges = [h.exchange  for h in holdings]

    ltp_map = fetch_ltp_batch(symbols, exchanges)

    fetched = sum(1 for v in ltp_map.values() if v is not None)
    logger.info("LTP enrichment: %d/%d symbols resolved", fetched, len(symbols))

    return [_enrich(h, ltp_map) for h in holdings]


@router.post("", response_model=HoldingResponse, status_code=status.HTTP_201_CREATED)
async def create_holding(
    body: HoldingCreate,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
):
    existing = await db.execute(
        select(Holding).where(Holding.symbol == body.symbol.upper())
    )
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail=f"Symbol {body.symbol} already exists.")

    holding = Holding(
        symbol=body.symbol.upper(),
        stock_name=body.stock_name,
        quantity=body.quantity,
        average_buy_price=body.average_buy_price,
        invested_amount=calc_invested(body.quantity, body.average_buy_price),
        exchange=body.exchange.upper(),
    )
    db.add(holding)
    await db.commit()
    await db.refresh(holding)
    return holding


@router.patch("/{symbol}", response_model=HoldingResponse)
async def add_shares(
    symbol: str,
    body: HoldingUpdate,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
):
    result = await db.execute(select(Holding).where(Holding.symbol == symbol.upper()))
    holding = result.scalar_one_or_none()
    if not holding:
        raise HTTPException(status_code=404, detail="Symbol not found")

    new_qty = holding.quantity + body.additional_quantity
    new_avg = (
        (holding.quantity * holding.average_buy_price)
        + (body.additional_quantity * body.buy_price)
    ) / new_qty

    holding.quantity = round(new_qty, 4)
    holding.average_buy_price = round(new_avg, 4)
    holding.invested_amount = calc_invested(holding.quantity, holding.average_buy_price)
    await db.commit()
    await db.refresh(holding)
    return holding


@router.post("/{symbol}/sell", status_code=status.HTTP_200_OK)
async def sell_shares(
    symbol: str,
    body: HoldingSellRequest,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
):
    result = await db.execute(select(Holding).where(Holding.symbol == symbol.upper()))
    holding = result.scalar_one_or_none()
    if not holding:
        raise HTTPException(status_code=404, detail="Symbol not found")

    if body.sell_quantity > holding.quantity:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot sell {body.sell_quantity}; only {holding.quantity} held.",
        )

    new_qty = round(holding.quantity - body.sell_quantity, 4)
    if new_qty == 0:
        await db.delete(holding)
        await db.commit()
        return {"message": f"{symbol.upper()} fully sold and removed.", "removed": True}

    holding.quantity = new_qty
    holding.invested_amount = calc_invested(new_qty, holding.average_buy_price)
    await db.commit()
    await db.refresh(holding)
    return {"message": f"Sold {body.sell_quantity} of {symbol.upper()}.", "removed": False}


@router.delete("/{symbol}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_holding(
    symbol: str,
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
):
    result = await db.execute(select(Holding).where(Holding.symbol == symbol.upper()))
    holding = result.scalar_one_or_none()
    if not holding:
        raise HTTPException(status_code=404, detail="Symbol not found")
    await db.delete(holding)
    await db.commit()


@router.post("/upload-csv", status_code=status.HTTP_200_OK)
async def upload_zerodha_csv(
    file: UploadFile = File(...),
    db: AsyncSession = Depends(get_db),
    _: str = Depends(get_current_user),
):
    if not file.filename.endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files accepted")

    content = await file.read()
    try:
        df = pd.read_csv(io.StringIO(content.decode("utf-8")))
    except Exception:
        raise HTTPException(status_code=400, detail="Could not parse CSV")

    col_map = {"Instrument": "symbol", "Avg. cost": "average_buy_price", "Qty.": "quantity"}
    missing = [c for c in col_map if c not in df.columns]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing columns: {missing}. Found: {list(df.columns)}")

    df = df.rename(columns=col_map)[["symbol", "average_buy_price", "quantity"]]
    df = df.dropna(subset=["symbol", "average_buy_price", "quantity"])
    df["symbol"] = df["symbol"].str.strip().str.upper()

    await db.execute(delete(Holding))

    added = 0
    for _, row in df.iterrows():
        sym = str(row["symbol"])
        qty = float(row["quantity"])
        avg = float(row["average_buy_price"])
        db.add(Holding(
            symbol=sym,
            stock_name=sym,
            quantity=round(qty, 4),
            average_buy_price=round(avg, 4),
            invested_amount=calc_invested(qty, avg),
            exchange="NSE",
        ))
        added += 1

    await db.commit()
    return {"message": f"Portfolio replaced. {added} holdings loaded.", "added": added, "updated": 0}
