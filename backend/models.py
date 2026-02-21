from datetime import datetime, date

from sqlalchemy import Column, Integer, String, Text, DateTime, Date, UniqueConstraint, Index, ForeignKey, Float
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

    # Subscription fields
    subscription_tier = Column(String, default="free")  # "free" or "pro"
    subscription_expires_at = Column(DateTime, nullable=True)
    razorpay_customer_id = Column(String, nullable=True)
    razorpay_subscription_id = Column(String, nullable=True)

    # Relationship to followed brands
    follows = relationship("UserFollow", back_populates="user", cascade="all, delete-orphan")

    @property
    def is_pro(self):
        if self.subscription_tier != "pro":
            return False
        if self.subscription_expires_at and self.subscription_expires_at < datetime.utcnow():
            return False
        return True


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


class UserBookmark(Base):
    __tablename__ = "user_bookmarks"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    email_id = Column(Integer, ForeignKey("emails.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", backref="bookmarks")
    email = relationship("Email")

    __table_args__ = (
        UniqueConstraint("user_id", "email_id", name="uq_user_email_bookmark"),
    )


class UserDailyUsage(Base):
    __tablename__ = "user_daily_usage"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    usage_date = Column(Date, nullable=False)
    html_views = Column(Integer, default=0)
    brand_views = Column(Integer, default=0)

    __table_args__ = (
        UniqueConstraint("user_id", "usage_date", name="uq_user_daily_usage"),
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

