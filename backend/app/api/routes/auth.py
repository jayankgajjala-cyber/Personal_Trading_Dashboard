import logging
import time
import uuid as uuid_lib

from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.security import OAuth2PasswordBearer
from sqlalchemy import select, delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.security import (
    get_password_hash, verify_password,
    generate_otp, create_access_token, decode_token,
)
from app.db.session import get_db
from app.models.otp_store import OtpStore
from app.models.user import User
from app.schemas.holding import (
    SignupRequest, LoginRequest, OTPVerifyRequest,
    TokenResponse, UserResponse,
)
from app.services.email import send_otp_email

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/auth", tags=["auth"])
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/auth/token")

OTP_TTL_SECONDS = 300


# ── Dependency ────────────────────────────────────────────────────────────────

async def get_current_user(
    token: str = Depends(oauth2_scheme),
    db: AsyncSession = Depends(get_db),
) -> User:
    """Decode JWT → look up User row → return User object."""
    payload = decode_token(token)
    if not payload or "sub" not in payload:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    user_id: str = payload["sub"]
    try:
        uid = uuid_lib.UUID(user_id)
    except ValueError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Malformed token")

    result = await db.execute(select(User).where(User.id == uid))
    user = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")
    return user


# ── OTP helpers ───────────────────────────────────────────────────────────────

async def _issue_otp(username: str, db: AsyncSession) -> str:
    otp     = generate_otp()
    expires = time.time() + OTP_TTL_SECONDS
    await db.execute(delete(OtpStore).where(OtpStore.username == username))
    db.add(OtpStore(username=username, otp=str(otp), expires=expires))
    await db.commit()
    logger.info("OTP issued for '%s'", username)
    return otp


async def _verify_otp_db(username: str, otp: str, db: AsyncSession) -> bool:
    result = await db.execute(select(OtpStore).where(OtpStore.username == username))
    record = result.scalar_one_or_none()
    if not record:
        return False
    if time.time() > record.expires:
        await db.execute(delete(OtpStore).where(OtpStore.username == username))
        await db.commit()
        return False
    if str(record.otp).strip() != str(otp).strip():
        return False
    await db.execute(delete(OtpStore).where(OtpStore.username == username))
    await db.commit()
    return True


# ── Endpoints ─────────────────────────────────────────────────────────────────

@router.post("/signup", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def signup(req: SignupRequest, db: AsyncSession = Depends(get_db)):
    """Register a new user."""
    existing = await db.execute(select(User).where(User.username == req.username))
    if existing.scalar_one_or_none():
        raise HTTPException(status_code=409, detail="Username already taken.")

    user = User(
        username        = req.username,
        hashed_password = get_password_hash(req.password),
        email           = req.email,
    )
    db.add(user)
    await db.commit()
    await db.refresh(user)
    logger.info("New user registered: %s", req.username)
    return user


@router.post("/login")
async def login(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Step 1: Validate credentials against DB, send OTP to registered email."""
    result = await db.execute(select(User).where(User.username == req.username))
    user   = result.scalar_one_or_none()

    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    if not user.email:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="No email registered. Cannot send OTP. Please contact support.",
        )

    otp  = await _issue_otp(req.username, db)
    sent = send_otp_email(otp, user.email)
    if not sent:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to send OTP email. Check Resend configuration.",
        )
    return {"message": "OTP sent to registered email."}


@router.post("/resend-otp")
async def resend_otp(req: LoginRequest, db: AsyncSession = Depends(get_db)):
    """Resend a fresh OTP — re-validates credentials to prevent abuse."""
    result = await db.execute(select(User).where(User.username == req.username))
    user   = result.scalar_one_or_none()

    if not user or not verify_password(req.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    if not user.email:
        raise HTTPException(status_code=422, detail="No email registered.")

    otp  = await _issue_otp(req.username, db)
    sent = send_otp_email(otp, user.email)
    if not sent:
        raise HTTPException(status_code=503, detail="Failed to send OTP email.")
    return {"message": "Fresh OTP sent to registered email."}


@router.post("/verify-otp", response_model=TokenResponse)
async def verify_otp_endpoint(req: OTPVerifyRequest, db: AsyncSession = Depends(get_db)):
    """Step 2: Verify OTP → issue JWT with user UUID as sub."""
    result = await db.execute(select(User).where(User.username == req.username))
    user   = result.scalar_one_or_none()
    if not user:
        raise HTTPException(status_code=400, detail="Invalid username")

    if not await _verify_otp_db(req.username, req.otp, db):
        raise HTTPException(status_code=400, detail="Invalid or expired OTP")

    # sub = user UUID (not username) — stable across username changes
    token = create_access_token({"sub": str(user.id)})
    return TokenResponse(access_token=token)


@router.get("/me", response_model=UserResponse)
async def me(current_user: User = Depends(get_current_user)):
    return current_user
