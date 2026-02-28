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
    subscription_tier = Column(String, default="free")  # "free", "starter", "pro", "agency"
    subscription_expires_at = Column(DateTime, nullable=True)
    razorpay_customer_id = Column(String, nullable=True)
    razorpay_subscription_id = Column(String, nullable=True)
    billing_cycle = Column(String, nullable=True)  # "monthly" or "annual"

    # Reverse trial
    trial_ends_at = Column(DateTime, nullable=True)  # Set to NOW+14 days on signup
    trial_emails_sent = Column(String, nullable=True)  # JSON: which reminder emails sent

    # Relationship to followed brands
    follows = relationship("UserFollow", back_populates="user", cascade="all, delete-orphan")

    @property
    def is_pro(self):
        """Backward-compatible check: is user on Pro or Agency (or trial)?"""
        from .plans import get_effective_plan
        return get_effective_plan(self) in ("pro", "agency")

    @property
    def effective_plan(self):
        """Get the user's effective plan accounting for trial."""
        from .plans import get_effective_plan
        return get_effective_plan(self)

    @property
    def is_on_trial(self):
        """Check if user is currently on a free trial."""
        if self.subscription_tier in ("starter", "pro", "agency"):
            if not self.subscription_expires_at or self.subscription_expires_at > datetime.utcnow():
                return False  # Has active paid plan, not on trial
        return bool(self.trial_ends_at and self.trial_ends_at > datetime.utcnow())


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
    html_exports = Column(Integer, default=0)  # Monthly counter for template exports
    exports_reset_at = Column(DateTime, nullable=True)  # When to reset export counter
    ai_generations = Column(Integer, default=0)  # Monthly counter for AI email generations

    __table_args__ = (
        UniqueConstraint("user_id", "usage_date", name="uq_user_daily_usage"),
    )


class Collection(Base):
    """User-created email collections (swipe files)."""
    __tablename__ = "collections"

    id = Column(Integer, primary_key=True, index=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    user = relationship("User", backref="collections")
    emails = relationship("CollectionEmail", back_populates="collection", cascade="all, delete-orphan")


class CollectionEmail(Base):
    """Join table for emails in a collection."""
    __tablename__ = "collection_emails"

    id = Column(Integer, primary_key=True, index=True)
    collection_id = Column(Integer, ForeignKey("collections.id", ondelete="CASCADE"), nullable=False)
    email_id = Column(Integer, ForeignKey("emails.id", ondelete="CASCADE"), nullable=False)
    added_at = Column(DateTime, default=datetime.utcnow)

    collection = relationship("Collection", back_populates="emails")
    email = relationship("Email")

    __table_args__ = (
        UniqueConstraint("collection_id", "email_id", name="uq_collection_email"),
    )


class ContactSalesInquiry(Base):
    """Agency tier sales inquiries."""
    __tablename__ = "contact_sales"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    email = Column(String, nullable=False)
    company = Column(String, nullable=True)
    message = Column(Text, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class NewsletterSubscriber(Base):
    __tablename__ = "newsletter_subscribers"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)


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


class TweetQueue(Base):
    """Queue of generated tweets awaiting approval and posting."""
    __tablename__ = "tweet_queue"

    id = Column(Integer, primary_key=True, index=True)
    content = Column(Text, nullable=False)  # Tweet text (max 280 chars)
    tweet_type = Column(String, nullable=False, index=True)  # daily_digest, weekly_digest, brand_spotlight, subject_line_insight
    status = Column(String, default="draft", index=True)  # draft, approved, posted, rejected
    scheduled_for = Column(DateTime, nullable=True)
    posted_at = Column(DateTime, nullable=True)
    twitter_id = Column(String, nullable=True)  # Twitter's tweet ID after posting
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

