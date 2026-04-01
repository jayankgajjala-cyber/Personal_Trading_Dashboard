from sqlalchemy import Column, String, Float
from app.db.session import Base


class OtpStore(Base):
    __tablename__ = "otp_store"

    username = Column(String, primary_key=True, nullable=False)
    otp = Column(String, nullable=False)
    expires = Column(Float, nullable=False)  # unix timestamp
