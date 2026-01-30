from datetime import datetime

from sqlalchemy import Column, Integer, String, Text, DateTime, UniqueConstraint, Index, ForeignKey, Float
from sqlalchemy.orm import relationship

from .db import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    password_hash = Column(String, nullable=True)  # Null for OAuth-only users
    name = Column(String, nullable=True)
    google_id = Column(String, unique=True, nullable=True, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationship to followed brands
    follows = relationship("UserFollow", back_populates="user", cascade="all, delete-orphan")


class UserFollow(Base):
    __tablename__ = "user_follows"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    brand_name = Column(String, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Relationship back to user
    user = relationship("User", back_populates="follows")

    __table_args__ = (
        UniqueConstraint("user_id", "brand_name", name="uq_user_brand_follow"),
    )


class Email(Base):
    __tablename__ = "emails"

    id = Column(Integer, primary_key=True, index=True)
    gmail_id = Column(String, unique=True, index=True, nullable=False)
    subject = Column(String, index=True, nullable=False)
    sender = Column(String, index=True, nullable=True)
    brand = Column(String, index=True, nullable=True)
    category = Column(String, index=True, nullable=True)
    type = Column(String, index=True, nullable=True)
    industry = Column(String, index=True, nullable=True)  # New field for industry classification
    received_at = Column(DateTime, index=True, default=datetime.utcnow)
    html = Column(Text, nullable=False)
    preview = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("gmail_id", name="uq_emails_gmail_id"),
        Index("ix_emails_brand_type", "brand", "type"),
        Index("ix_emails_industry", "industry"),
    )


class BrandClassification(Base):
    """
    Cache for AI-powered brand classifications.
    Stores industry classification for each brand to avoid repeated API calls.
    """
    __tablename__ = "brand_classifications"

    id = Column(Integer, primary_key=True, index=True)
    brand_name = Column(String, unique=True, nullable=False, index=True)
    industry = Column(String, nullable=False)
    confidence = Column(Float, default=1.0)  # 0-1 confidence score from AI
    classified_by = Column(String, default="ai")  # "ai", "manual", "keyword"
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

