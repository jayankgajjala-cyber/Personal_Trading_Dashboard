import uuid
from sqlalchemy import Column, String, DateTime, func
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import relationship

from app.db.session import Base


class User(Base):
    __tablename__ = "users"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    username        = Column(String, unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)
    email           = Column(String, nullable=True)
    created_at      = Column(DateTime(timezone=True), server_default=func.now())

    holdings = relationship("Holding", back_populates="owner", lazy="noload")
