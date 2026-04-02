import uuid
from sqlalchemy import Column, String, Float, DateTime, ForeignKey, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class Holding(Base):
    __tablename__ = "holdings"

    id                  = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    user_id             = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=True, index=True)
    symbol              = Column(String, nullable=False, index=True)
    stock_name          = Column(String, nullable=False)
    quantity            = Column(Float, nullable=False)
    average_buy_price   = Column(Float, nullable=False)
    invested_amount     = Column(Float, nullable=False)
    exchange            = Column(String, nullable=False, server_default="NSE")

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

    owner = relationship("User", back_populates="holdings", lazy="noload")
