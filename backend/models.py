from datetime import datetime

from sqlalchemy import Column, Integer, String, Text, DateTime, UniqueConstraint, Index

from .db import Base


class Email(Base):
    __tablename__ = "emails"

    id = Column(Integer, primary_key=True, index=True)
    gmail_id = Column(String, unique=True, index=True, nullable=False)
    subject = Column(String, index=True, nullable=False)
    sender = Column(String, index=True, nullable=True)
    brand = Column(String, index=True, nullable=True)
    category = Column(String, index=True, nullable=True)
    type = Column(String, index=True, nullable=True)
    received_at = Column(DateTime, index=True, default=datetime.utcnow)
    html = Column(Text, nullable=False)
    preview = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("gmail_id", name="uq_emails_gmail_id"),
        Index("ix_emails_brand_type", "brand", "type"),
    )

