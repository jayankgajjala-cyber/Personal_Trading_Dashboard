import logging
import time

from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import settings
from app.core.security import generate_otp, create_access_token, decode_token
from app.db.session import get_db
from app.models.otp_store import OtpStore
from app.schemas.holding import LoginRequest, OTPVerifyRequest, TokenResponse
from app.services.email import send_otp_email

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

OTP_TTL_SECONDS = 300


def get_current_user(token: str = Depends(oauth2_scheme)) -> str:
    payload = decode_token(token)
    if not payload or "sub" not in payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return payload["sub"]


async def _issue_otp(username: str, db: AsyncSession) -> str:
    """Generate OTP, persist to DB (upsert), return the OTP string."""
    otp = generate_otp()
    expires = time.time() + OTP_TTL_SECONDS

    # Upsert: delete existing then insert fresh
    await db.execute(delete(OtpStore).where(OtpStore.username == username))
    db.add(OtpStore(username=username, otp=str(otp), expires=expires))
    await db.commit()
    logger.info("OTP issued for '%s', expires in %ds", username, OTP_TTL_SECONDS)
    return otp


async def _verify_otp_db(username: str, otp: str, db: AsyncSession) -> bool:
    """
    Check OTP against DB. Deletes record on success or expiry.
    Does NOT delete on wrong attempt — allows retries within TTL.
    """
    result = await db.execute(select(OtpStore).where(OtpStore.username == username))
    record = result.scalar_one_or_none()

    if not record:
        logger.warning("OTP verify: no record for '%s'", username)
        return False

    if time.time() > record.expires:
        await db.execute(delete(OtpStore).where(OtpStore.username == username))
        await db.commit()
        logger.warning("OTP verify: expired for '%s'", username)
        return False

    if str(record.otp).strip() != str(otp).strip():
        logger.warning("OTP verify: mismatch for '%s'", username)
        return False  # keep record; user can retry

    # Success — consume
    await db.execute(delete(OtpStore).where(OtpStore.username == username))
    await db.commit()
    logger.info("OTP verify: success for '%s'", username)
    return True


@router.post("/login")
async def login(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Step 1: Validate credentials, send OTP."""
    if req.username != settings.APP_USERNAME or req.password != settings.APP_PASSWORD:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    otp = await _issue_otp(req.username, db)
    sent = send_otp_email(otp)
    if not sent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to send OTP email. Check Resend configuration.",
        )
    return {"message": "OTP sent to registered email."}


@router.post("/resend-otp")
async def resend_otp(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Resend a fresh OTP. Requires credentials to prevent abuse."""
    if req.username != settings.APP_USERNAME or req.password != settings.APP_PASSWORD:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    otp = await _issue_otp(req.username, db)
    sent = send_otp_email(otp)
    if not sent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to send OTP email.",
        )
    return {"message": "Fresh OTP sent to registered email."}


@router.post("/verify-otp", response_model=TokenResponse)
async def verify_otp_endpoint(req: OTPVerifyRequest, db: AsyncSession = Depends(get_db)):
    """Step 2: Verify OTP, issue JWT."""
    if req.username != settings.APP_USERNAME:
        raise HTTPException(status_code=400, detail="Invalid username")
    if not await _verify_otp_db(req.username, req.otp, db):
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")
    token = create_access_token({"sub": req.username})
    return TokenResponse(access_token=token)


@router.get("/me")
async def me(username: str = Depends(get_current_user)):
    return {"username": username}
