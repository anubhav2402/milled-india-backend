import os
from typing import List, Optional
from datetime import datetime, timedelta

from fastapi import Depends, FastAPI, HTTPException, Query, Request, status
from fastapi.middleware.cors import CORSMiddleware
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded
from sqlalchemy.orm import Session
from sqlalchemy import inspect, text

from . import models, schemas
from .db import Base, SessionLocal, engine
from .utils import extract_preview_image_url
from .auth import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
    get_optional_user,
    get_pro_user,
    get_admin_user,
    get_or_create_daily_usage,
    verify_google_token,
    require_plan,
)
from .plans import get_effective_plan, PLAN_LIMITS, check_numeric_limit, get_limit
from .twitter import generate_tweet_content, post_tweet, is_twitter_configured
from .payments import (
    create_subscription,
    verify_payment_signature,
    verify_webhook_signature,
    get_subscription_details,
    cancel_subscription,
    get_plan_details,
)

# Create tables on startup (simple for local dev)
Base.metadata.create_all(bind=engine)

# Auto-migration: Add industry column if it doesn't exist
def run_migrations():
    """Add new columns to existing tables if they don't exist."""
    with engine.connect() as conn:
        # Check if industry column exists
        try:
            result = conn.execute(text(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name='emails' AND column_name='industry'"
            ))
            if result.fetchone() is None:
                # Column doesn't exist, add it
                conn.execute(text("ALTER TABLE emails ADD COLUMN industry VARCHAR"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_emails_industry ON emails(industry)"))
                conn.commit()
                print("Migration: Added 'industry' column to emails table")
        except Exception as e:
            print(f"Migration check failed (might be SQLite): {e}")
            # For SQLite, try a different approach
            try:
                conn.execute(text("ALTER TABLE emails ADD COLUMN industry VARCHAR"))
                conn.commit()
                print("Migration: Added 'industry' column to emails table (SQLite)")
            except Exception:
                pass  # Column likely already exists

        # Add subscription columns to users table
        subscription_columns = [
            ("subscription_tier", "VARCHAR DEFAULT 'free'"),
            ("subscription_expires_at", "TIMESTAMP"),
            ("razorpay_customer_id", "VARCHAR"),
            ("razorpay_subscription_id", "VARCHAR"),
        ]
        for col_name, col_type in subscription_columns:
            try:
                conn.execute(text(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}"))
                conn.commit()
                print(f"Migration: Added '{col_name}' column to users table")
            except Exception:
                pass  # Column likely already exists

        # Create user_daily_usage table if it doesn't exist
        try:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS user_daily_usage ("
                "id SERIAL PRIMARY KEY, "
                "user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, "
                "usage_date DATE NOT NULL, "
                "html_views INTEGER DEFAULT 0, "
                "brand_views INTEGER DEFAULT 0, "
                "UNIQUE(user_id, usage_date))"
            ))
            conn.commit()
            print("Migration: Ensured user_daily_usage table exists")
        except Exception as e:
            print(f"Migration user_daily_usage (trying SQLite): {e}")
            try:
                conn.execute(text(
                    "CREATE TABLE IF NOT EXISTS user_daily_usage ("
                    "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                    "user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, "
                    "usage_date DATE NOT NULL, "
                    "html_views INTEGER DEFAULT 0, "
                    "brand_views INTEGER DEFAULT 0, "
                    "UNIQUE(user_id, usage_date))"
                ))
                conn.commit()
            except Exception:
                pass

        # --- 4-tier pricing migrations ---

        # New columns on users table
        tier_user_columns = [
            ("billing_cycle", "VARCHAR"),
            ("trial_ends_at", "TIMESTAMP"),
            ("trial_emails_sent", "VARCHAR"),
        ]
        for col_name, col_type in tier_user_columns:
            try:
                conn.execute(text(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}"))
                conn.commit()
                print(f"Migration: Added '{col_name}' column to users table")
            except Exception:
                pass  # Column likely already exists

        # New columns on user_daily_usage table
        usage_columns = [
            ("html_exports", "INTEGER DEFAULT 0"),
            ("exports_reset_at", "TIMESTAMP"),
        ]
        for col_name, col_type in usage_columns:
            try:
                conn.execute(text(f"ALTER TABLE user_daily_usage ADD COLUMN {col_name} {col_type}"))
                conn.commit()
                print(f"Migration: Added '{col_name}' column to user_daily_usage table")
            except Exception:
                pass

        # Create collections table
        try:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS collections ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE, "
                "name VARCHAR NOT NULL, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            ))
            conn.commit()
            print("Migration: Ensured collections table exists")
        except Exception:
            pass

        # Create collection_emails table
        try:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS collection_emails ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "collection_id INTEGER NOT NULL REFERENCES collections(id) ON DELETE CASCADE, "
                "email_id INTEGER NOT NULL REFERENCES emails(id) ON DELETE CASCADE, "
                "added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                "UNIQUE(collection_id, email_id))"
            ))
            conn.commit()
            print("Migration: Ensured collection_emails table exists")
        except Exception:
            pass

        # Create contact_sales table
        try:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS contact_sales ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "name VARCHAR NOT NULL, "
                "email VARCHAR NOT NULL, "
                "company VARCHAR, "
                "message TEXT, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            ))
            conn.commit()
            print("Migration: Ensured contact_sales table exists")
        except Exception:
            pass

        # Create newsletter_subscribers table if missing
        try:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS newsletter_subscribers ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "email VARCHAR NOT NULL UNIQUE, "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            ))
            conn.commit()
        except Exception:
            pass

        # Create brand_classifications table if missing
        try:
            conn.execute(text(
                "CREATE TABLE IF NOT EXISTS brand_classifications ("
                "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                "brand_name VARCHAR NOT NULL UNIQUE, "
                "industry VARCHAR NOT NULL, "
                "confidence REAL DEFAULT 1.0, "
                "classified_by VARCHAR DEFAULT 'ai', "
                "created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, "
                "updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)"
            ))
            conn.commit()
        except Exception:
            pass

    # Create tweet_queue table
    if not inspect(engine).has_table("tweet_queue"):
        models.TweetQueue.__table__.create(engine)

run_migrations()


def _user_out(user: models.User) -> schemas.UserOut:
    """Build a UserOut response from a User model, including trial/plan fields."""
    return schemas.UserOut(
        id=user.id,
        email=user.email,
        name=user.name,
        subscription_tier=user.subscription_tier or "free",
        effective_plan=user.effective_plan,
        is_pro=user.is_pro,
        is_on_trial=user.is_on_trial,
        trial_ends_at=user.trial_ends_at,
    )


app = FastAPI(title="Milled India API", version="0.1.0")

# Rate limiting
limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)

# CORS middleware - allow frontend to make requests
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "https://mailmuse.vercel.app",
        "https://milled-india-frontend.vercel.app",
        "https://mailmuse.in",
        "https://www.mailmuse.in",
        # Allow any Vercel preview deployments
        "https://*.vercel.app",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app|https://(www\.)?mailmuse\.in",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/admin/create-tables")
def create_tables(current_user: models.User = Depends(get_admin_user)):
    """Create all database tables. Use this to ensure user tables exist."""
    try:
        Base.metadata.create_all(bind=engine)
        return {"message": "Tables created successfully", "tables": list(Base.metadata.tables.keys())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create tables: {str(e)}")


@app.get("/admin/users")
def list_users(
    skip: int = 0,
    limit: int = 100,
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    """List all registered users."""
    users = db.query(models.User).order_by(models.User.created_at.desc()).offset(skip).limit(limit).all()
    total = db.query(models.User).count()

    return {
        "total": total,
        "users": [
            {
                "id": u.id,
                "email": u.email,
                "name": u.name,
                "subscription_tier": u.subscription_tier,
                "effective_plan": u.effective_plan,
                "is_pro": u.is_pro,
                "is_on_trial": u.is_on_trial,
                "created_at": u.created_at.isoformat() if u.created_at else None,
            }
            for u in users
        ],
    }


@app.get("/admin/newsletter-subscribers")
def list_newsletter_subscribers(
    skip: int = 0,
    limit: int = 100,
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    """List all newsletter subscribers."""
    subs = db.query(models.NewsletterSubscriber).order_by(models.NewsletterSubscriber.created_at.desc()).offset(skip).limit(limit).all()
    total = db.query(models.NewsletterSubscriber).count()

    return {
        "total": total,
        "subscribers": [
            {
                "id": s.id,
                "email": s.email,
                "created_at": s.created_at.isoformat() if s.created_at else None,
            }
            for s in subs
        ],
    }


# ============ Authentication Endpoints ============

@app.post("/auth/register", response_model=schemas.TokenResponse)
@limiter.limit("5/minute")
def register(request: Request, user_data: schemas.UserCreate, db: Session = Depends(get_db)):
    """Register a new user with email and password."""
    try:
        # Check if email already exists
        existing_user = db.query(models.User).filter(models.User.email == user_data.email).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        
        # Create new user with 14-day Pro trial
        user = models.User(
            email=user_data.email,
            password_hash=hash_password(user_data.password),
            name=user_data.name,
            trial_ends_at=datetime.utcnow() + timedelta(days=14),
        )
        db.add(user)
        db.commit()
        db.refresh(user)

        # Create access token
        token = create_access_token(user.id, user.email)

        return schemas.TokenResponse(
            access_token=token,
            user=_user_out(user)
        )
    except HTTPException:
        raise
    except Exception as e:
        import traceback
        print(f"Registration error: {str(e)}")
        print(traceback.format_exc())
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Registration failed: {str(e)}"
        )


@app.post("/auth/login", response_model=schemas.TokenResponse)
@limiter.limit("10/minute")
def login(request: Request, credentials: schemas.UserLogin, db: Session = Depends(get_db)):
    """Login with email and password."""
    user = db.query(models.User).filter(models.User.email == credentials.email).first()
    
    if not user or not user.password_hash:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    if not verify_password(credentials.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid email or password"
        )
    
    # Create access token
    token = create_access_token(user.id, user.email)
    
    return schemas.TokenResponse(
        access_token=token,
        user=_user_out(user)
    )


@app.post("/auth/google", response_model=schemas.TokenResponse)
@limiter.limit("10/minute")
def google_auth(request: Request, auth_data: schemas.GoogleAuth, db: Session = Depends(get_db)):
    """Login or register with Google OAuth."""
    # Verify Google token
    google_info = verify_google_token(auth_data.token)
    if not google_info:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid Google token"
        )
    
    # Check if user exists by Google ID
    user = db.query(models.User).filter(models.User.google_id == google_info["google_id"]).first()
    
    if not user:
        # Check if email already exists (maybe registered with password)
        user = db.query(models.User).filter(models.User.email == google_info["email"]).first()
        if user:
            # Link Google account to existing user
            user.google_id = google_info["google_id"]
            if not user.name and google_info.get("name"):
                user.name = google_info["name"]
        else:
            # Create new user with 14-day Pro trial
            user = models.User(
                email=google_info["email"],
                google_id=google_info["google_id"],
                name=google_info.get("name"),
                trial_ends_at=datetime.utcnow() + timedelta(days=14),
            )
            db.add(user)
        
        db.commit()
        db.refresh(user)
    
    # Create access token
    token = create_access_token(user.id, user.email)
    
    return schemas.TokenResponse(
        access_token=token,
        user=_user_out(user)
    )


@app.get("/auth/me", response_model=schemas.UserOut)
def get_me(current_user: models.User = Depends(get_current_user)):
    """Get current authenticated user info."""
    return _user_out(current_user)


# ============ Newsletter Endpoints ============

@app.post("/newsletter/subscribe")
@limiter.limit("5/minute")
def newsletter_subscribe(request: Request, payload: schemas.NewsletterSubscribeRequest, db: Session = Depends(get_db)):
    """Subscribe an email to the newsletter."""
    email = payload.email.strip().lower()
    existing = db.query(models.NewsletterSubscriber).filter_by(email=email).first()
    if existing:
        return {"message": "Already subscribed"}
    subscriber = models.NewsletterSubscriber(email=email)
    db.add(subscriber)
    db.commit()
    return {"message": "Subscribed successfully"}


# ============ Subscription Endpoints ============

@app.get("/subscription/plans")
def get_plans():
    """Get available subscription plans and pricing."""
    return get_plan_details()


@app.post("/subscription/create")
def create_user_subscription(
    data: dict,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Create a Razorpay subscription for the current user."""
    billing = data.get("plan", "monthly")
    if billing not in ("monthly", "annual"):
        raise HTTPException(status_code=400, detail="Plan must be 'monthly' or 'annual'")
    tier = data.get("tier", "pro")
    if tier not in ("starter", "pro"):
        raise HTTPException(status_code=400, detail="Tier must be 'starter' or 'pro'")

    try:
        subscription = create_subscription(current_user.email, billing, tier)
        # Store subscription ID on user
        current_user.razorpay_subscription_id = subscription["id"]
        db.commit()
        return {
            "subscription_id": subscription["id"],
            "short_url": subscription.get("short_url"),
        }
    except ValueError as e:
        raise HTTPException(status_code=503, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create subscription: {str(e)}")


@app.post("/subscription/verify")
def verify_subscription(
    data: dict,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Verify payment and activate Pro subscription."""
    payment_id = data.get("razorpay_payment_id")
    subscription_id = data.get("razorpay_subscription_id")
    signature = data.get("razorpay_signature")

    if not all([payment_id, subscription_id, signature]):
        raise HTTPException(status_code=400, detail="Missing payment verification fields")

    if not verify_payment_signature(payment_id, subscription_id, signature):
        raise HTTPException(status_code=400, detail="Invalid payment signature")

    # Activate subscription — tier comes from the plan the user selected
    tier = data.get("tier", "pro")
    if tier not in ("starter", "pro"):
        tier = "pro"
    billing = data.get("billing_cycle", "monthly")

    current_user.subscription_tier = tier
    current_user.billing_cycle = billing
    current_user.razorpay_subscription_id = subscription_id
    # Set expiry: 35 days for monthly, 370 days for annual (buffer for renewals)
    days = 370 if billing == "annual" else 35
    current_user.subscription_expires_at = datetime.utcnow() + timedelta(days=days)
    db.commit()

    return {
        "message": f"{tier.title()} subscription activated",
        "subscription_tier": tier,
        "effective_plan": current_user.effective_plan,
        "is_pro": current_user.is_pro,
        "expires_at": current_user.subscription_expires_at.isoformat(),
    }


@app.get("/subscription/status")
def subscription_status(
    current_user: models.User = Depends(get_current_user),
):
    """Get current user's subscription status."""
    return {
        "subscription_tier": current_user.subscription_tier or "free",
        "effective_plan": current_user.effective_plan,
        "is_pro": current_user.is_pro,
        "is_on_trial": current_user.is_on_trial,
        "trial_ends_at": current_user.trial_ends_at.isoformat() if current_user.trial_ends_at else None,
        "expires_at": current_user.subscription_expires_at.isoformat() if current_user.subscription_expires_at else None,
        "razorpay_subscription_id": current_user.razorpay_subscription_id,
    }


@app.post("/subscription/cancel")
def cancel_user_subscription(
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Cancel the current user's Pro subscription."""
    if not current_user.razorpay_subscription_id:
        raise HTTPException(status_code=400, detail="No active subscription to cancel")

    try:
        cancel_subscription(current_user.razorpay_subscription_id)
    except Exception as e:
        print(f"Razorpay cancel error: {e}")
        # Continue anyway — user wants to cancel

    # Keep pro access until expiry but mark for cancellation
    current_user.razorpay_subscription_id = None
    db.commit()

    return {
        "message": "Subscription cancelled. Pro access continues until expiry.",
        "expires_at": current_user.subscription_expires_at.isoformat() if current_user.subscription_expires_at else None,
    }


@app.post("/webhook/razorpay")
async def razorpay_webhook(request_obj: dict, db: Session = Depends(get_db)):
    """Handle Razorpay webhook events for subscription renewals and cancellations."""
    from fastapi import Request
    # Note: For production, verify webhook signature using raw body
    # For now, process the event payload

    event = request_obj.get("event", "")
    payload = request_obj.get("payload", {})

    if event == "subscription.charged":
        # Renewal successful — extend subscription
        subscription_entity = payload.get("subscription", {}).get("entity", {})
        sub_id = subscription_entity.get("id")
        if sub_id:
            user = db.query(models.User).filter(
                models.User.razorpay_subscription_id == sub_id
            ).first()
            if user:
                # Extend subscription — keep user's existing tier
                days = 370 if user.billing_cycle == "annual" else 35
                user.subscription_expires_at = datetime.utcnow() + timedelta(days=days)
                db.commit()

    elif event == "subscription.cancelled":
        subscription_entity = payload.get("subscription", {}).get("entity", {})
        sub_id = subscription_entity.get("id")
        if sub_id:
            user = db.query(models.User).filter(
                models.User.razorpay_subscription_id == sub_id
            ).first()
            if user:
                user.razorpay_subscription_id = None
                db.commit()

    return {"status": "ok"}


# ============ User Follows Endpoints ============

@app.get("/user/follows", response_model=schemas.UserFollowsResponse)
def get_user_follows(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get list of brands the current user follows."""
    follows = db.query(models.UserFollow).filter(
        models.UserFollow.user_id == current_user.id
    ).all()
    return schemas.UserFollowsResponse(follows=[f.brand_name for f in follows])


@app.post("/user/follows/{brand_name}")
def follow_brand(brand_name: str, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Follow a brand. Free users limited to 3 follows."""
    # Check if already following
    existing = db.query(models.UserFollow).filter(
        models.UserFollow.user_id == current_user.id,
        models.UserFollow.brand_name == brand_name
    ).first()

    if existing:
        return {"message": "Already following this brand"}

    # Tier-based follow limits
    plan = get_effective_plan(current_user)
    follow_limit = get_limit(plan, "follows")
    if follow_limit is not None:
        follow_count = db.query(models.UserFollow).filter(
            models.UserFollow.user_id == current_user.id
        ).count()
        if follow_count >= follow_limit:
            result = check_numeric_limit(plan, "follows", follow_count)
            raise HTTPException(
                status_code=403,
                detail=f"Your plan allows up to {follow_limit} brand follows. Upgrade to {result['upgrade_to'] or 'a higher plan'} for more.",
            )

    # Create new follow
    follow = models.UserFollow(user_id=current_user.id, brand_name=brand_name)
    db.add(follow)
    db.commit()

    return {"message": f"Now following {brand_name}"}


@app.delete("/user/follows/{brand_name}")
def unfollow_brand(brand_name: str, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Unfollow a brand."""
    follow = db.query(models.UserFollow).filter(
        models.UserFollow.user_id == current_user.id,
        models.UserFollow.brand_name == brand_name
    ).first()
    
    if not follow:
        raise HTTPException(status_code=404, detail="Not following this brand")
    
    db.delete(follow)
    db.commit()
    
    return {"message": f"Unfollowed {brand_name}"}


# ============ User Bookmarks Endpoints ============

@app.get("/user/bookmarks/ids")
def get_bookmark_ids(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get list of bookmarked email IDs for the current user."""
    bookmarks = db.query(models.UserBookmark.email_id).filter(
        models.UserBookmark.user_id == current_user.id
    ).all()
    return {"ids": [b.email_id for b in bookmarks]}


@app.get("/user/bookmarks")
def get_bookmarks(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get all bookmarked emails with full data."""
    bookmarks = db.query(models.UserBookmark).filter(
        models.UserBookmark.user_id == current_user.id
    ).order_by(models.UserBookmark.created_at.desc()).all()

    results = []
    for bm in bookmarks:
        email = bm.email
        if email:
            results.append({
                "id": email.id,
                "subject": email.subject,
                "brand": email.brand,
                "industry": email.industry,
                "type": email.type,
                "preview": email.preview,
                "received_at": email.received_at.isoformat() if email.received_at else None,
                "bookmarked_at": bm.created_at.isoformat() if bm.created_at else None,
            })
    return {"bookmarks": results}


@app.post("/user/bookmarks")
def add_bookmark(data: dict, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Bookmark an email. Free users limited to 10 bookmarks."""
    email_id = data.get("email_id")
    if not email_id:
        raise HTTPException(status_code=400, detail="email_id is required")

    # Check email exists
    email = db.query(models.Email).filter(models.Email.id == email_id).first()
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    # Check if already bookmarked
    existing = db.query(models.UserBookmark).filter(
        models.UserBookmark.user_id == current_user.id,
        models.UserBookmark.email_id == email_id
    ).first()

    if existing:
        return {"message": "Already bookmarked"}

    # Tier-based bookmark limits
    plan = get_effective_plan(current_user)
    bookmark_limit = get_limit(plan, "bookmarks")
    if bookmark_limit is not None:
        bookmark_count = db.query(models.UserBookmark).filter(
            models.UserBookmark.user_id == current_user.id
        ).count()
        if bookmark_count >= bookmark_limit:
            result = check_numeric_limit(plan, "bookmarks", bookmark_count)
            raise HTTPException(
                status_code=403,
                detail=f"Your plan allows up to {bookmark_limit} bookmarks. Upgrade to {result['upgrade_to'] or 'a higher plan'} for more.",
            )

    bookmark = models.UserBookmark(user_id=current_user.id, email_id=email_id)
    db.add(bookmark)
    db.commit()

    return {"message": "Bookmarked"}


@app.delete("/user/bookmarks/{email_id}")
def remove_bookmark(email_id: int, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Remove a bookmark."""
    bookmark = db.query(models.UserBookmark).filter(
        models.UserBookmark.user_id == current_user.id,
        models.UserBookmark.email_id == email_id
    ).first()

    if not bookmark:
        raise HTTPException(status_code=404, detail="Bookmark not found")

    db.delete(bookmark)
    db.commit()

    return {"message": "Bookmark removed"}


# ============ Collections Endpoints ============

@app.get("/user/collections")
def list_collections(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Get user's collections with email counts."""
    collections = db.query(models.Collection).filter(
        models.Collection.user_id == current_user.id
    ).order_by(models.Collection.created_at.desc()).all()

    return {
        "collections": [
            {
                "id": c.id,
                "name": c.name,
                "email_count": len(c.emails),
                "created_at": c.created_at.isoformat() if c.created_at else None,
            }
            for c in collections
        ]
    }


@app.post("/user/collections")
def create_collection(data: schemas.CollectionCreate, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Create a new collection. Tier-based limits apply."""
    plan = get_effective_plan(current_user)
    coll_limit = get_limit(plan, "collections")
    if coll_limit is not None:
        count = db.query(models.Collection).filter(
            models.Collection.user_id == current_user.id
        ).count()
        if count >= coll_limit:
            result = check_numeric_limit(plan, "collections", count)
            raise HTTPException(
                status_code=403,
                detail=f"Your plan allows up to {coll_limit} collections. Upgrade to {result['upgrade_to'] or 'a higher plan'} for more.",
            )

    collection = models.Collection(user_id=current_user.id, name=data.name)
    db.add(collection)
    db.commit()
    db.refresh(collection)

    return {"id": collection.id, "name": collection.name, "message": "Collection created"}


@app.delete("/user/collections/{collection_id}")
def delete_collection(collection_id: int, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Delete a collection (cascade deletes its emails)."""
    collection = db.query(models.Collection).filter(
        models.Collection.id == collection_id,
        models.Collection.user_id == current_user.id,
    ).first()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    db.delete(collection)
    db.commit()
    return {"message": "Collection deleted"}


@app.post("/user/collections/{collection_id}/emails")
def add_email_to_collection(
    collection_id: int,
    data: dict,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Add an email to a collection. Tier-based per-collection limits apply."""
    email_id = data.get("email_id")
    if not email_id:
        raise HTTPException(status_code=400, detail="email_id is required")

    collection = db.query(models.Collection).filter(
        models.Collection.id == collection_id,
        models.Collection.user_id == current_user.id,
    ).first()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    # Check per-collection email limit
    plan = get_effective_plan(current_user)
    per_coll_limit = get_limit(plan, "emails_per_collection")
    if per_coll_limit is not None:
        count = db.query(models.CollectionEmail).filter(
            models.CollectionEmail.collection_id == collection_id
        ).count()
        if count >= per_coll_limit:
            raise HTTPException(
                status_code=403,
                detail=f"This collection can hold up to {per_coll_limit} emails on your plan. Upgrade for more.",
            )

    # Check not already added
    existing = db.query(models.CollectionEmail).filter(
        models.CollectionEmail.collection_id == collection_id,
        models.CollectionEmail.email_id == email_id,
    ).first()
    if existing:
        return {"message": "Email already in collection"}

    ce = models.CollectionEmail(collection_id=collection_id, email_id=email_id)
    db.add(ce)
    db.commit()
    return {"message": "Email added to collection"}


@app.delete("/user/collections/{collection_id}/emails/{email_id}")
def remove_email_from_collection(
    collection_id: int,
    email_id: int,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Remove an email from a collection."""
    ce = db.query(models.CollectionEmail).filter(
        models.CollectionEmail.collection_id == collection_id,
        models.CollectionEmail.email_id == email_id,
    ).first()
    if not ce:
        raise HTTPException(status_code=404, detail="Email not in collection")

    # Verify ownership
    collection = db.query(models.Collection).filter(
        models.Collection.id == collection_id,
        models.Collection.user_id == current_user.id,
    ).first()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    db.delete(ce)
    db.commit()
    return {"message": "Email removed from collection"}


@app.get("/user/collections/{collection_id}/emails")
def get_collection_emails(
    collection_id: int,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get all emails in a collection."""
    collection = db.query(models.Collection).filter(
        models.Collection.id == collection_id,
        models.Collection.user_id == current_user.id,
    ).first()
    if not collection:
        raise HTTPException(status_code=404, detail="Collection not found")

    results = []
    for ce in collection.emails:
        email = ce.email
        if email:
            results.append({
                "id": email.id,
                "subject": email.subject,
                "brand": email.brand,
                "industry": email.industry,
                "type": email.type,
                "preview": email.preview,
                "received_at": email.received_at.isoformat() if email.received_at else None,
                "added_at": ce.added_at.isoformat() if ce.added_at else None,
            })

    return {"collection": {"id": collection.id, "name": collection.name}, "emails": results}


# ============ Usage & Trial Endpoints ============

@app.get("/user/usage")
def get_user_usage(
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Get current usage stats for the authenticated user."""
    from datetime import date as date_type

    plan = get_effective_plan(current_user)
    usage = get_or_create_daily_usage(db, current_user.id)

    follow_count = db.query(models.UserFollow).filter(
        models.UserFollow.user_id == current_user.id
    ).count()
    bookmark_count = db.query(models.UserBookmark).filter(
        models.UserBookmark.user_id == current_user.id
    ).count()
    collection_count = db.query(models.Collection).filter(
        models.Collection.user_id == current_user.id
    ).count()

    def _usage_dict(feature, current):
        limit = get_limit(plan, feature)
        return {
            "used": current,
            "limit": limit,
            "remaining": None if limit is None else max(0, limit - current),
        }

    return {
        "plan": plan,
        "is_on_trial": current_user.is_on_trial,
        "trial_ends_at": current_user.trial_ends_at.isoformat() if current_user.trial_ends_at else None,
        "email_views": _usage_dict("email_views_per_day", usage.html_views),
        "brand_views": _usage_dict("brand_pages_per_day", usage.brand_views),
        "html_exports": _usage_dict("html_exports_per_month", usage.html_exports or 0),
        "collections": _usage_dict("collections", collection_count),
        "follows": _usage_dict("follows", follow_count),
        "bookmarks": _usage_dict("bookmarks", bookmark_count),
    }


@app.get("/subscription/trial-status")
def get_trial_status(current_user: models.User = Depends(get_current_user)):
    """Get trial information for the current user."""
    is_on_trial = current_user.is_on_trial
    days_left = 0
    if is_on_trial and current_user.trial_ends_at:
        delta = current_user.trial_ends_at - datetime.utcnow()
        days_left = max(0, delta.days)

    return {
        "is_on_trial": is_on_trial,
        "trial_ends_at": current_user.trial_ends_at.isoformat() if current_user.trial_ends_at else None,
        "days_left": days_left,
        "effective_plan": current_user.effective_plan,
    }


@app.post("/contact-sales")
@limiter.limit("3/minute")
def contact_sales(request: Request, data: schemas.ContactSalesRequest, db: Session = Depends(get_db)):
    """Submit an Agency tier sales inquiry."""
    inquiry = models.ContactSalesInquiry(
        name=data.name,
        email=data.email,
        company=data.company,
        message=data.message,
    )
    db.add(inquiry)
    db.commit()
    return {"message": "Thank you! Our team will get back to you within 24 hours."}


@app.post("/emails/export-html")
def export_email_html(
    data: dict,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Export email HTML template. Tier-based monthly limits apply."""
    email_id = data.get("email_id")
    if not email_id:
        raise HTTPException(status_code=400, detail="email_id is required")

    email = db.query(models.Email).filter(models.Email.id == email_id).first()
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    plan = get_effective_plan(current_user)
    export_limit = get_limit(plan, "html_exports_per_month")

    if export_limit is not None:
        if export_limit == 0:
            raise HTTPException(
                status_code=403,
                detail="HTML export is not available on your plan. Upgrade to Starter or higher.",
            )
        usage = get_or_create_daily_usage(db, current_user.id)
        exports_used = usage.html_exports or 0
        if exports_used >= export_limit:
            raise HTTPException(
                status_code=403,
                detail=f"Monthly export limit reached ({export_limit}/month). Upgrade for more exports.",
            )
        usage.html_exports = exports_used + 1
        # Set reset date if not already set (first of next month)
        if not usage.exports_reset_at:
            now = datetime.utcnow()
            if now.month == 12:
                reset = now.replace(year=now.year + 1, month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            else:
                reset = now.replace(month=now.month + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            usage.exports_reset_at = reset
        db.commit()

    return {"html": email.html, "subject": email.subject, "brand": email.brand}


# ============ Email Endpoints ============

@app.get("/emails", response_model=List[schemas.EmailListOut])
@limiter.limit("30/minute")
def list_emails(
    request: Request,
    brand: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),
    industry: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    skip: int = 0,
    limit: Optional[int] = Query(default=None),  # No limit by default - returns all
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    """
    Get emails (lightweight - no HTML), sorted by newest first.
    Free users and unauthenticated users see only last 30 days.
    """
    query = db.query(models.Email).order_by(models.Email.received_at.desc())

    # Tier-based archive depth
    plan = get_effective_plan(current_user) if current_user else "free"
    archive_days = get_limit(plan, "archive_days")
    if archive_days is not None:
        cutoff = datetime.utcnow() - timedelta(days=archive_days)
        query = query.filter(models.Email.received_at >= cutoff)

    if brand:
        query = query.filter(models.Email.brand == brand)
    if type:
        query = query.filter(models.Email.type == type)
    if industry:
        query = query.filter(models.Email.industry == industry)
    if q:
        like = f"%{q}%"
        query = query.filter(
            models.Email.subject.ilike(like) | models.Email.preview.ilike(like)
        )

    query = query.offset(skip)
    if limit is not None:
        query = query.limit(limit)
    
    # Only fetch needed columns (exclude HTML for speed)
    emails = query.all()
    
    # Return lightweight response (no HTML parsing - very fast)
    return [
        schemas.EmailListOut(
            id=email.id,
            gmail_id=email.gmail_id,
            subject=email.subject,
            sender=email.sender,
            brand=email.brand,
            category=email.category,
            type=email.type,
            industry=email.industry,
            received_at=email.received_at,
            preview=email.preview,
            preview_image_url=None,  # Skip for now - too slow to compute on-the-fly
        )
        for email in emails
    ]


@app.post("/emails/html")
@limiter.limit("20/minute")
def get_emails_html(
    request: Request,
    ids: List[int],
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    """
    Get HTML content for specific email IDs (for lazy loading previews).
    Free users limited to 10 HTML views/day. Requires login.
    """
    if not current_user:
        raise HTTPException(status_code=401, detail="Login required to view email content")

    if len(ids) > 50:
        ids = ids[:50]

    plan = get_effective_plan(current_user)
    view_limit = get_limit(plan, "email_views_per_day")
    if view_limit is not None:
        usage = get_or_create_daily_usage(db, current_user.id)
        remaining = view_limit - usage.html_views
        if remaining <= 0:
            raise HTTPException(
                status_code=403,
                detail=f"Daily email view limit reached ({view_limit}/day). Upgrade for more access.",
                headers={"X-Upgrade-To": check_numeric_limit(plan, "email_views_per_day", usage.html_views).get("upgrade_to", "")},
            )
        ids = ids[:remaining]
        usage.html_views += len(ids)
        db.commit()

    emails = db.query(models.Email).filter(models.Email.id.in_(ids)).all()
    return {email.id: email.html for email in emails}


@app.get("/emails/{email_id}/html")
@limiter.limit("30/minute")
def get_email_html(
    request: Request,
    email_id: int,
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    """Get just the HTML content for a single email. Tier-based daily limits."""
    if current_user:
        plan = get_effective_plan(current_user)
        view_limit = get_limit(plan, "email_views_per_day")
        if view_limit is not None:
            usage = get_or_create_daily_usage(db, current_user.id)
            if usage.html_views >= view_limit:
                raise HTTPException(
                    status_code=403,
                    detail=f"Daily email view limit reached ({view_limit}/day). Upgrade for more access.",
                )
            usage.html_views += 1
            db.commit()

    email = db.query(models.Email).filter(models.Email.id == email_id).first()
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")
    return {"html": email.html}


@app.get("/industries", response_model=List[str])
def list_industries(db: Session = Depends(get_db)):
    """Get list of all industries that have emails."""
    result = db.query(models.Email.industry).filter(
        models.Email.industry.isnot(None)
    ).distinct().all()
    return sorted([r[0] for r in result if r[0]])


@app.get("/emails/count")
def count_emails(db: Session = Depends(get_db)):
    """Get total number of emails in the database."""
    total = db.query(models.Email).count()
    return {"total": total}


@app.get("/emails/ids")
def list_email_ids(db: Session = Depends(get_db)):
    """Get all email IDs and last modified dates for sitemap generation."""
    results = db.query(models.Email.id, models.Email.received_at).order_by(
        models.Email.received_at.desc()
    ).all()
    return [{"id": r[0], "received_at": r[1].isoformat() if r[1] else None} for r in results]


@app.get("/brands", response_model=List[str])
def list_brands(db: Session = Depends(get_db)):
    """Get list of all unique brands."""
    result = db.query(models.Email.brand).filter(
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).distinct().all()
    return sorted([r[0] for r in result if r[0]])


@app.get("/emails/{email_id}", response_model=schemas.EmailOut)
def get_email(
    email_id: int,
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db),
):
    email = db.query(models.Email).filter(models.Email.id == email_id).first()
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    html_content = email.html

    # Enforce tier-based HTML view limit
    if current_user:
        plan = get_effective_plan(current_user)
        view_limit = get_limit(plan, "email_views_per_day")
        if view_limit is not None:
            usage = get_or_create_daily_usage(db, current_user.id)
            if usage.html_views >= view_limit:
                html_content = "<div style='text-align:center;padding:60px 20px;font-family:sans-serif;'><h2>Daily limit reached</h2><p>You've viewed your daily limit of emails. Upgrade for more access.</p><a href='/pricing' style='color:#c45a3c;font-weight:600;'>View Plans</a></div>"
            else:
                usage.html_views += 1
                db.commit()

    # Add preview_image_url
    email_dict = {
        "id": email.id,
        "gmail_id": email.gmail_id,
        "subject": email.subject,
        "sender": email.sender,
        "brand": email.brand,
        "category": email.category,
        "type": email.type,
        "industry": email.industry,
        "received_at": email.received_at,
        "preview": email.preview,
        "html": html_content,
        "preview_image_url": extract_preview_image_url(email.html),
    }
    return schemas.EmailOut(**email_dict)


@app.post("/admin/update-industries")
def update_industries(current_user: models.User = Depends(get_admin_user), db: Session = Depends(get_db)):
    """
    Update industry field for all existing emails based on brand name.
    This is an admin endpoint to backfill industry data.
    """
    # Import here to avoid circular imports
    import sys
    sys.path.insert(0, '/opt/render/project/src')
    try:
        from engine import extract_industry
    except ImportError:
        # Try relative import for local dev
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from engine import extract_industry
    
    emails = db.query(models.Email).filter(models.Email.industry.is_(None)).all()
    updated = 0
    unmatched_brands = set()
    
    for email in emails:
        # Use smart extraction with content analysis
        industry = extract_industry(
            brand_name=email.brand,
            subject=email.subject,
            preview=email.preview,
            html=email.html
        )
        if industry:
            email.industry = industry
            updated += 1
        else:
            if email.brand:
                unmatched_brands.add(email.brand)
    
    db.commit()
    
    return {
        "message": f"Updated {updated} emails with industry data",
        "total_processed": len(emails),
        "updated": updated,
        "unmatched_brands": list(unmatched_brands)[:50],  # Show first 50 unmatched brands
    }


@app.get("/admin/brands-without-industry")
def get_brands_without_industry(current_user: models.User = Depends(get_admin_user), db: Session = Depends(get_db)):
    """
    Get list of all unique brands that don't have an industry assigned.
    """
    result = db.query(models.Email.brand).filter(
        models.Email.industry.is_(None),
        models.Email.brand.isnot(None)
    ).distinct().all()
    
    brands = sorted([r[0] for r in result if r[0]])
    return {
        "count": len(brands),
        "brands": brands
    }


@app.post("/admin/update-brands")
def update_brands(current_user: models.User = Depends(get_admin_user), db: Session = Depends(get_db)):
    """
    Re-extract brand names for all emails using the improved extraction logic.
    Uses sender, HTML content, and subject line for smart brand detection.
    """
    # Import here to avoid circular imports
    import sys
    sys.path.insert(0, '/opt/render/project/src')
    try:
        from engine import extract_brand, extract_industry
    except ImportError:
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from engine import extract_brand, extract_industry
    
    # Get emails with "Unknown" brand or None
    emails = db.query(models.Email).filter(
        (models.Email.brand == "Unknown") | 
        (models.Email.brand == "unknown") |
        (models.Email.brand.is_(None))
    ).all()
    
    updated = 0
    results = []
    
    for email in emails:
        old_brand = email.brand
        
        # Re-extract brand using all available info
        new_brand = extract_brand(
            sender=email.sender,
            html=email.html,
            subject=email.subject
        )
        
        if new_brand and new_brand != "Unknown" and new_brand != old_brand:
            email.brand = new_brand
            # Also update industry based on new brand
            email.industry = extract_industry(
                brand_name=new_brand,
                subject=email.subject,
                preview=email.preview,
                html=email.html
            )
            updated += 1
            results.append({
                "id": email.id,
                "old_brand": old_brand,
                "new_brand": new_brand,
                "industry": email.industry
            })
    
    db.commit()
    
    return {
        "message": f"Updated {updated} email brand names",
        "total_processed": len(emails),
        "updated": updated,
        "samples": results[:20]  # Show first 20 updates
    }


@app.post("/admin/reprocess-all")
def reprocess_all(current_user: models.User = Depends(get_admin_user), db: Session = Depends(get_db)):
    """
    Reprocess ALL emails - re-extract both brand and industry for every email.
    Use this for a full refresh of the classification.
    """
    import sys
    sys.path.insert(0, '/opt/render/project/src')
    try:
        from engine import extract_brand, extract_industry
    except ImportError:
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from engine import extract_brand, extract_industry
    
    emails = db.query(models.Email).all()
    
    brand_updated = 0
    industry_updated = 0
    
    for email in emails:
        # Re-extract brand
        new_brand = extract_brand(
            sender=email.sender,
            html=email.html,
            subject=email.subject
        )
        
        if new_brand and new_brand != email.brand:
            email.brand = new_brand
            brand_updated += 1
        
        # Re-extract industry
        new_industry = extract_industry(
            brand_name=email.brand,
            subject=email.subject,
            preview=email.preview,
            html=email.html
        )
        
        if new_industry and new_industry != email.industry:
            email.industry = new_industry
            industry_updated += 1
    
    db.commit()
    
    return {
        "message": "Full reprocessing complete",
        "total_emails": len(emails),
        "brands_updated": brand_updated,
        "industries_updated": industry_updated
    }


@app.delete("/admin/clear-all-emails")
def clear_all_emails(current_user: models.User = Depends(get_admin_user), db: Session = Depends(get_db)):
    """
    Delete ALL emails from the database.
    Use this before re-ingesting to get fresh HTML with updated cleaning.
    WARNING: This is irreversible!
    """
    count = db.query(models.Email).count()
    db.query(models.Email).delete()
    db.commit()
    
    return {
        "message": f"Deleted {count} emails. Run the cron job with GMAIL_FETCH_ALL=true to re-ingest.",
        "deleted": count
    }


@app.post("/admin/update-campaign-types")
def update_campaign_types(current_user: models.User = Depends(get_admin_user), db: Session = Depends(get_db)):
    """
    Backfill campaign types for all existing emails.
    """
    import sys
    sys.path.insert(0, '/opt/render/project/src')
    try:
        from engine import extract_campaign_type
    except ImportError:
        import os
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from engine import extract_campaign_type
    
    # Get emails without campaign type
    emails = db.query(models.Email).filter(models.Email.type.is_(None)).all()
    updated = 0
    
    for email in emails:
        campaign_type = extract_campaign_type(
            subject=email.subject,
            preview=email.preview,
            html=email.html
        )
        if campaign_type:
            email.type = campaign_type
            updated += 1
    
    db.commit()
    
    return {
        "message": f"Updated {updated} emails with campaign types",
        "total_processed": len(emails),
        "updated": updated
    }


# ============ AI Classification Endpoints ============

@app.get("/admin/brand-classifications")
def get_brand_classifications(
    skip: int = 0,
    limit: int = 100,
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Get all cached brand classifications.
    """
    classifications = db.query(models.BrandClassification).offset(skip).limit(limit).all()
    
    return {
        "total": db.query(models.BrandClassification).count(),
        "classifications": [
            {
                "brand_name": c.brand_name,
                "industry": c.industry,
                "confidence": c.confidence,
                "classified_by": c.classified_by,
                "created_at": c.created_at.isoformat() if c.created_at else None,
                "updated_at": c.updated_at.isoformat() if c.updated_at else None,
            }
            for c in classifications
        ]
    }


@app.put("/admin/brand-classifications/{brand_name}")
def update_brand_classification(
    brand_name: str,
    industry: str = Query(..., description="New industry classification"),
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Manually override a brand's industry classification.
    """
    # Validate industry
    valid_industries = [
        "Beauty & Personal Care", "Women's Fashion", "Men's Fashion",
        "Food & Beverages", "Travel & Hospitality", "Electronics & Gadgets",
        "Home & Living", "Health & Wellness", "Finance & Fintech",
        "Kids & Baby", "Sports & Fitness", "Entertainment", "General Retail",
    ]
    
    if industry not in valid_industries:
        raise HTTPException(
            status_code=400, 
            detail=f"Invalid industry. Must be one of: {valid_industries}"
        )
    
    # Find or create classification
    classification = db.query(models.BrandClassification).filter(
        models.BrandClassification.brand_name.ilike(brand_name)
    ).first()
    
    if classification:
        classification.industry = industry
        classification.confidence = 1.0
        classification.classified_by = "manual"
    else:
        classification = models.BrandClassification(
            brand_name=brand_name,
            industry=industry,
            confidence=1.0,
            classified_by="manual",
        )
        db.add(classification)
    
    # Also update all emails from this brand
    updated_emails = db.query(models.Email).filter(
        models.Email.brand.ilike(brand_name)
    ).update({"industry": industry}, synchronize_session=False)
    
    db.commit()
    
    return {
        "message": f"Updated classification for {brand_name}",
        "industry": industry,
        "emails_updated": updated_emails
    }


@app.get("/admin/test-ai")
def test_ai_classification(current_user: models.User = Depends(get_admin_user)):
    """
    Test if AI classification is working. Returns detailed error info.
    """
    import os
    import traceback
    
    result = {
        "openai_key_set": bool(os.getenv("OPENAI_API_KEY")),
        "openai_key_prefix": os.getenv("OPENAI_API_KEY", "")[:10] + "..." if os.getenv("OPENAI_API_KEY") else None,
    }
    
    try:
        from .ai_classifier import is_ai_available, classify_brand_with_ai
        result["is_ai_available"] = is_ai_available()
        
        # Try a test classification
        test_result = classify_brand_with_ai("Nykaa", "50% off on all beauty products", "Shop skincare, makeup and more")
        result["test_classification"] = test_result
        result["status"] = "success"
    except Exception as e:
        result["status"] = "error"
        result["error"] = str(e)
        result["traceback"] = traceback.format_exc()
    
    return result


@app.post("/admin/reclassify-keywords")
def reclassify_with_keywords(
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Re-classify all brands using expanded keyword mappings (NO AI).
    This is free and fast.
    """
    import sys
    sys.path.insert(0, '/opt/render/project/src')
    
    from engine import BRAND_INDUSTRY_MAPPING, extract_industry
    from sqlalchemy import func
    
    # Get all unique brands
    brands = db.query(models.Email.brand).filter(
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).distinct().all()
    
    brand_names = [b[0] for b in brands]
    
    results = {
        "total_brands": len(brand_names),
        "classified": 0,
        "unclassified": [],
        "classifications": {}
    }
    
    for brand_name in brand_names:
        # Get a sample email for keyword context
        sample_email = db.query(models.Email).filter(
            models.Email.brand.ilike(brand_name)
        ).first()
        
        subject = sample_email.subject if sample_email else None
        preview = sample_email.preview if sample_email else None
        html = sample_email.html if sample_email else None
        
        # Classify using keywords only (use_ai=False)
        industry = extract_industry(
            brand_name=brand_name,
            subject=subject,
            preview=preview,
            html=html,
            db_session=db,
            use_ai=False
        )
        
        if industry:
            # Update all emails from this brand
            db.query(models.Email).filter(
                models.Email.brand.ilike(brand_name)
            ).update({"industry": industry}, synchronize_session=False)
            
            results["classified"] += 1
            results["classifications"][brand_name] = industry
        else:
            results["unclassified"].append(brand_name)
    
    db.commit()
    
    return results


@app.post("/admin/reclassify-campaign-types")
def reclassify_campaign_types_endpoint(
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Re-classify campaign types for all emails using expanded keyword matching.
    This is free and fast (no AI).
    """
    import sys
    sys.path.insert(0, '/opt/render/project/src')
    
    from engine import extract_campaign_type
    from collections import Counter
    
    # Get all emails
    emails = db.query(models.Email).all()
    
    results = {
        "total_emails": len(emails),
        "classified": 0,
        "unclassified": 0,
        "distribution": Counter()
    }
    
    for email in emails:
        # Classify using keywords only (use_ai=False)
        campaign_type = extract_campaign_type(
            subject=email.subject,
            preview=email.preview,
            html=email.html,
            brand_name=email.brand,
            use_ai=False
        )
        
        if campaign_type:
            email.type = campaign_type
            results["classified"] += 1
            results["distribution"][campaign_type] += 1
        else:
            results["unclassified"] += 1
            results["distribution"]["Unclassified"] += 1
    
    db.commit()
    
    # Convert Counter to dict for JSON serialization
    results["distribution"] = dict(results["distribution"])
    
    return results


@app.post("/admin/reclassify-brand/{brand_name}")
def reclassify_single_brand(
    brand_name: str,
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Re-classify a single brand using AI.
    """
    try:
        from .ai_classifier import classify_brand_with_ai, is_ai_available
        
        if not is_ai_available():
            raise HTTPException(
                status_code=503,
                detail="AI classification not available. Set OPENAI_API_KEY."
            )
        
        # Get sample email for context
        sample_email = db.query(models.Email).filter(
            models.Email.brand.ilike(brand_name)
        ).first()
        
        subject = sample_email.subject if sample_email else None
        preview = sample_email.preview if sample_email else None
        
        # Classify with AI
        result = classify_brand_with_ai(brand_name, subject, preview)
        industry = result.get("industry", "General Retail")
        confidence = result.get("confidence", 0.8)
        
        # Update or create classification
        classification = db.query(models.BrandClassification).filter(
            models.BrandClassification.brand_name.ilike(brand_name)
        ).first()
        
        if classification:
            classification.industry = industry
            classification.confidence = confidence
            classification.classified_by = "ai"
        else:
            classification = models.BrandClassification(
                brand_name=brand_name,
                industry=industry,
                confidence=confidence,
                classified_by="ai",
            )
            db.add(classification)
        
        # Update all emails from this brand
        updated_emails = db.query(models.Email).filter(
            models.Email.brand.ilike(brand_name)
        ).update({"industry": industry}, synchronize_session=False)
        
        db.commit()
        
        return {
            "brand": brand_name,
            "industry": industry,
            "confidence": confidence,
            "emails_updated": updated_emails
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Classification failed: {str(e)}")


@app.post("/admin/reclassify-brands")
def reclassify_all_brands(
    force: bool = Query(default=False, description="Force reclassify even if already classified"),
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Re-classify all brands using AI.
    This will update the brand_classifications table and all email industries.
    
    Args:
        force: If True, reclassify all brands. If False, only classify unclassified brands.
    """
    try:
        from .ai_classifier import classify_brand_with_ai, is_ai_available
        from sqlalchemy import func
        
        if not is_ai_available():
            raise HTTPException(
                status_code=503,
                detail="AI classification not available. Set OPENAI_API_KEY."
            )
        
        # Get all unique brands
        brands = db.query(models.Email.brand).filter(
            models.Email.brand.isnot(None),
            models.Email.brand != "Unknown"
        ).distinct().all()
        
        brand_names = [b[0] for b in brands]
        
        results = {
            "total_brands": len(brand_names),
            "classified": 0,
            "skipped": 0,
            "errors": [],
        }
        
        for brand_name in brand_names:
            try:
                # Check if already classified
                existing = db.query(models.BrandClassification).filter(
                    models.BrandClassification.brand_name.ilike(brand_name)
                ).first()
                
                # Skip if already classified (unless force=True) or if manually set
                if existing and not force:
                    results["skipped"] += 1
                    continue
                if existing and existing.classified_by == "manual":
                    results["skipped"] += 1
                    continue
                
                # Get sample email for context
                sample_email = db.query(models.Email).filter(
                    models.Email.brand.ilike(brand_name)
                ).first()
                
                subject = sample_email.subject if sample_email else None
                preview = sample_email.preview if sample_email else None
                
                # Classify with AI
                result = classify_brand_with_ai(brand_name, subject, preview)
                industry = result.get("industry", "General Retail")
                confidence = result.get("confidence", 0.8)
                
                # Update or create classification
                if existing:
                    existing.industry = industry
                    existing.confidence = confidence
                    existing.classified_by = "ai"
                else:
                    classification = models.BrandClassification(
                        brand_name=brand_name,
                        industry=industry,
                        confidence=confidence,
                        classified_by="ai",
                    )
                    db.add(classification)
                
                # Update all emails from this brand
                db.query(models.Email).filter(
                    models.Email.brand.ilike(brand_name)
                ).update({"industry": industry}, synchronize_session=False)
                
                results["classified"] += 1
                
                # Commit periodically
                if results["classified"] % 10 == 0:
                    db.commit()
                    print(f"Classified {results['classified']} brands...")
                
            except Exception as e:
                results["errors"].append({"brand": brand_name, "error": str(e)})
        
        db.commit()
        
        return results
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Bulk classification failed: {str(e)}")


@app.delete("/admin/brand-classifications/{brand_name}")
def delete_brand_classification(
    brand_name: str,
    current_user: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db)
):
    """
    Delete a brand classification from the cache.
    The brand will be re-classified on next ingestion.
    """
    deleted = db.query(models.BrandClassification).filter(
        models.BrandClassification.brand_name.ilike(brand_name)
    ).delete(synchronize_session=False)
    
    db.commit()
    
    return {
        "message": f"Deleted classification for {brand_name}" if deleted else f"No classification found for {brand_name}",
        "deleted": deleted > 0
    }


@app.get("/brands/stats")
def get_brand_stats(
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Get statistics for all brands including send frequency and industry.
    Returns email count, average emails per week, and industry for each brand.
    If not authenticated, numeric stats are masked with "xx" but industry is always shown.
    """
    from sqlalchemy import func
    from datetime import datetime, timedelta

    # Get email counts per brand
    results = db.query(
        models.Email.brand,
        func.count(models.Email.id).label('email_count'),
        func.min(models.Email.received_at).label('first_email'),
        func.max(models.Email.received_at).label('last_email')
    ).filter(
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).group_by(models.Email.brand).all()

    # Get most common industry per brand
    industry_rows = db.query(
        models.Email.brand,
        models.Email.industry,
        func.count(models.Email.id).label('cnt')
    ).filter(
        models.Email.brand.isnot(None),
        models.Email.industry.isnot(None)
    ).group_by(models.Email.brand, models.Email.industry).all()

    brand_industries = {}
    for row in industry_rows:
        if row.brand not in brand_industries or row.cnt > brand_industries[row.brand][1]:
            brand_industries[row.brand] = (row.industry, row.cnt)

    brand_stats = {}
    is_authenticated = current_user is not None

    for row in results:
        brand = row.brand
        count = row.email_count
        first = row.first_email
        last = row.last_email

        # Calculate send frequency
        if first and last and first != last:
            days = (last - first).days
            if days > 0:
                emails_per_week = round((count / days) * 7, 1)
                if emails_per_week >= 7:
                    freq = f"{round(emails_per_week/7)}x/day"
                elif emails_per_week >= 1:
                    freq = f"{round(emails_per_week)}x/week"
                else:
                    emails_per_month = emails_per_week * 4
                    freq = f"{round(emails_per_month)}x/month"
            else:
                freq = "1x"
        else:
            freq = "1x"

        industry = brand_industries.get(brand, (None,))[0]

        # Mask numeric stats for non-authenticated users; industry is always public
        if is_authenticated:
            brand_stats[brand] = {
                "email_count": count,
                "send_frequency": freq,
                "industry": industry
            }
        else:
            brand_stats[brand] = {
                "email_count": "xx",
                "send_frequency": "xx",
                "industry": industry
            }

    return brand_stats


@app.get("/brands/by-industry/{industry}", response_model=List[str])
def get_brands_by_industry(industry: str, db: Session = Depends(get_db)):
    """Get all brand names in a specific industry."""
    from sqlalchemy import func
    result = db.query(models.Email.brand).filter(
        models.Email.industry.ilike(industry),
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).distinct().all()
    return sorted([r[0] for r in result if r[0]])


@app.get("/industries/{industry}/seo")
def get_industry_seo_data(industry: str, db: Session = Depends(get_db)):
    """Public, ungated data for industry SEO pages."""
    from sqlalchemy import func

    # Get brands in this industry
    brand_rows = db.query(models.Email.brand).filter(
        models.Email.industry.ilike(industry),
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).distinct().all()
    brands = sorted([r[0] for r in brand_rows if r[0]])

    if not brands:
        raise HTTPException(status_code=404, detail="Industry not found")

    # Total emails
    total_emails = db.query(func.count(models.Email.id)).filter(
        models.Email.industry.ilike(industry)
    ).scalar() or 0

    # Campaign type distribution (names only, no exact counts for SEO)
    campaign_rows = db.query(
        models.Email.type,
        func.count(models.Email.id)
    ).filter(
        models.Email.industry.ilike(industry),
        models.Email.type.isnot(None)
    ).group_by(models.Email.type).order_by(func.count(models.Email.id).desc()).all()
    top_campaign_types = [r[0] for r in campaign_rows[:5]]

    # Top brands by email count
    top_brand_rows = db.query(
        models.Email.brand,
        func.count(models.Email.id).label("count")
    ).filter(
        models.Email.industry.ilike(industry),
        models.Email.brand.isnot(None)
    ).group_by(models.Email.brand).order_by(func.count(models.Email.id).desc()).limit(5).all()
    top_brands = [{"brand": r[0], "email_count": r[1]} for r in top_brand_rows]

    return {
        "industry": industry,
        "total_brands": len(brands),
        "total_emails": total_emails,
        "brands": brands,
        "top_brands": top_brands,
        "top_campaign_types": top_campaign_types,
    }


# ============ Analytics Endpoints ============

def _extract_emojis(text: str) -> list:
    """Extract emojis from text."""
    import re
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # emoticons
        "\U0001F300-\U0001F5FF"  # symbols & pictographs
        "\U0001F680-\U0001F6FF"  # transport & map symbols
        "\U0001F1E0-\U0001F1FF"  # flags
        "\U00002702-\U000027B0"
        "\U000024C2-\U0001F251"
        "]+",
        flags=re.UNICODE
    )
    return emoji_pattern.findall(text)


def _get_day_name(day_num: int) -> str:
    """Convert day number to name."""
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    return days[day_num]


def _get_time_bucket(hour: int) -> str:
    """Convert hour to time bucket."""
    if 5 <= hour < 9:
        return "Early Morning (5-9am)"
    elif 9 <= hour < 12:
        return "Morning (9am-12pm)"
    elif 12 <= hour < 14:
        return "Lunch (12-2pm)"
    elif 14 <= hour < 17:
        return "Afternoon (2-5pm)"
    elif 17 <= hour < 20:
        return "Evening (5-8pm)"
    elif 20 <= hour < 23:
        return "Night (8-11pm)"
    else:
        return "Late Night (11pm-5am)"


# Sample brand that returns full analytics without auth (for ungated demo page)
SAMPLE_BRAND = os.getenv("SAMPLE_BRAND", "Nykaa").strip().lower()


@app.get("/analytics/brand/{brand_name:path}")
def get_brand_analytics(
    brand_name: str,
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Get detailed analytics for a specific brand.
    Includes campaign type breakdown, send timing patterns, subject line stats.
    """
    from sqlalchemy import func
    from collections import Counter
    import re

    is_authenticated = current_user is not None
    is_sample = brand_name.strip().lower() == SAMPLE_BRAND

    # Tier-based brand analytics view limits
    if current_user:
        plan = get_effective_plan(current_user)
        brand_limit = get_limit(plan, "brand_pages_per_day")
        if brand_limit is not None:
            usage = get_or_create_daily_usage(db, current_user.id)
            if usage.brand_views >= brand_limit:
                raise HTTPException(
                    status_code=403,
                    detail=f"Daily brand analytics limit reached ({brand_limit}/day). Upgrade for more access.",
                )
            usage.brand_views += 1
            db.commit()
    
    # Get all emails for this brand
    emails = db.query(models.Email).filter(
        models.Email.brand.ilike(brand_name)
    ).all()
    
    if not emails:
        raise HTTPException(status_code=404, detail=f"No emails found for brand: {brand_name}")
    
    # Basic stats
    total_emails = len(emails)
    
    # Campaign type breakdown
    campaign_types = Counter(e.type for e in emails if e.type)
    campaign_breakdown = {k: v for k, v in campaign_types.most_common()}
    
    # Industry
    industries = Counter(e.industry for e in emails if e.industry)
    primary_industry = industries.most_common(1)[0][0] if industries else None
    
    # Send day distribution
    day_distribution = Counter()
    time_distribution = Counter()
    
    for email in emails:
        if email.received_at:
            day_distribution[_get_day_name(email.received_at.weekday())] += 1
            time_distribution[_get_time_bucket(email.received_at.hour)] += 1
    
    # Subject line analysis
    subjects = [e.subject for e in emails if e.subject]
    avg_subject_length = round(sum(len(s) for s in subjects) / len(subjects), 1) if subjects else 0
    
    # Emoji usage
    emails_with_emoji = sum(1 for s in subjects if _extract_emojis(s))
    emoji_rate = round((emails_with_emoji / len(subjects)) * 100, 1) if subjects else 0
    
    # Top words in subjects (excluding common words)
    stop_words = {'the', 'a', 'an', 'is', 'are', 'and', 'or', 'to', 'for', 'of', 'in', 'on', 'at', 'your', 'you', 'we', 'our', 'this', 'that', 'it', 'with'}
    all_words = []
    for s in subjects:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', s.lower())
        all_words.extend(w for w in words if w not in stop_words)
    top_words = [word for word, count in Counter(all_words).most_common(10)]
    
    # Date range
    dates = [e.received_at for e in emails if e.received_at]
    first_email = min(dates) if dates else None
    last_email = max(dates) if dates else None
    
    # Calculate send frequency
    if first_email and last_email and first_email != last_email:
        days = (last_email - first_email).days
        if days > 0:
            emails_per_week = round((total_emails / days) * 7, 1)
        else:
            emails_per_week = total_emails
    else:
        emails_per_week = total_emails
    
    # Build response
    if is_authenticated or is_sample:
        return {
            "brand": brand_name,
            "total_emails": total_emails,
            "primary_industry": primary_industry,
            "emails_per_week": emails_per_week,
            "first_email": first_email.isoformat() if first_email else None,
            "last_email": last_email.isoformat() if last_email else None,
            "campaign_breakdown": campaign_breakdown,
            "send_day_distribution": dict(day_distribution),
            "send_time_distribution": dict(time_distribution),
            "subject_line_stats": {
                "avg_length": avg_subject_length,
                "emoji_usage_rate": emoji_rate,
                "top_words": top_words
            }
        }
    else:
        # Masked response for unauthenticated users
        return {
            "brand": brand_name,
            "total_emails": "xx",
            "primary_industry": primary_industry,
            "emails_per_week": "xx",
            "first_email": "xx",
            "last_email": "xx",
            "campaign_breakdown": {k: "xx" for k in campaign_breakdown},
            "send_day_distribution": "Login to view",
            "send_time_distribution": "Login to view",
            "subject_line_stats": "Login to view"
        }


@app.get("/analytics/industry/{industry}")
def get_industry_analytics(
    industry: str,
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Get analytics and benchmarks for a specific industry.
    """
    from sqlalchemy import func
    from collections import Counter
    
    is_authenticated = current_user is not None

    # Require at least Starter plan for analytics
    if current_user:
        plan = get_effective_plan(current_user)
        if plan == "free":
            raise HTTPException(status_code=403, detail="Starter plan or higher required for full analytics. Upgrade at /pricing")

    # Get all emails for this industry
    emails = db.query(models.Email).filter(
        models.Email.industry.ilike(industry)
    ).all()
    
    if not emails:
        raise HTTPException(status_code=404, detail=f"No emails found for industry: {industry}")
    
    total_emails = len(emails)
    
    # Brand breakdown
    brand_counts = Counter(e.brand for e in emails if e.brand and e.brand != "Unknown")
    top_brands = [{"brand": b, "count": c} for b, c in brand_counts.most_common(10)]
    total_brands = len(brand_counts)
    
    # Campaign type breakdown
    campaign_types = Counter(e.type for e in emails if e.type)
    campaign_mix = {k: round((v / total_emails) * 100, 1) for k, v in campaign_types.items()}
    
    # Send day distribution
    day_distribution = Counter()
    for email in emails:
        if email.received_at:
            day_distribution[_get_day_name(email.received_at.weekday())] += 1
    
    # Average send frequency per brand
    brand_email_counts = list(brand_counts.values())
    avg_emails_per_brand = round(sum(brand_email_counts) / len(brand_email_counts), 1) if brand_email_counts else 0
    
    # Subject line stats
    subjects = [e.subject for e in emails if e.subject]
    avg_subject_length = round(sum(len(s) for s in subjects) / len(subjects), 1) if subjects else 0
    
    if is_authenticated:
        return {
            "industry": industry,
            "total_emails": total_emails,
            "total_brands": total_brands,
            "top_brands": top_brands,
            "campaign_type_mix": campaign_mix,
            "send_day_distribution": dict(day_distribution),
            "avg_emails_per_brand": avg_emails_per_brand,
            "avg_subject_length": avg_subject_length
        }
    else:
        return {
            "industry": industry,
            "total_emails": "xx",
            "total_brands": total_brands,
            "top_brands": [{"brand": b["brand"], "count": "xx"} for b in top_brands],
            "campaign_type_mix": "Login to view",
            "send_day_distribution": "Login to view",
            "avg_emails_per_brand": "xx",
            "avg_subject_length": "xx"
        }


@app.get("/analytics/compare")
def compare_brands(
    brands: str = Query(..., description="Comma-separated brand names"),
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Compare multiple brands side by side.
    Pass brands as comma-separated: ?brands=nykaa,myntra,ajio
    """
    from sqlalchemy import func
    from collections import Counter
    
    is_authenticated = current_user is not None

    # Require at least Starter plan for analytics
    if current_user:
        plan = get_effective_plan(current_user)
        if plan == "free":
            raise HTTPException(status_code=403, detail="Starter plan or higher required for full analytics. Upgrade at /pricing")

    brand_list = [b.strip() for b in brands.split(",") if b.strip()]
    
    if len(brand_list) < 2:
        raise HTTPException(status_code=400, detail="Please provide at least 2 brands to compare")
    
    if len(brand_list) > 5:
        raise HTTPException(status_code=400, detail="Maximum 5 brands can be compared at once")
    
    comparison = {}
    
    for brand_name in brand_list:
        emails = db.query(models.Email).filter(
            models.Email.brand.ilike(brand_name)
        ).all()
        
        if not emails:
            comparison[brand_name] = {"error": "No emails found"}
            continue
        
        total = len(emails)
        
        # Campaign types
        campaign_types = Counter(e.type for e in emails if e.type)
        top_campaign = campaign_types.most_common(1)[0][0] if campaign_types else None
        
        # Industry
        industries = Counter(e.industry for e in emails if e.industry)
        industry = industries.most_common(1)[0][0] if industries else None
        
        # Subject length
        subjects = [e.subject for e in emails if e.subject]
        avg_length = round(sum(len(s) for s in subjects) / len(subjects), 1) if subjects else 0
        
        # Emoji rate
        emails_with_emoji = sum(1 for s in subjects if _extract_emojis(s))
        emoji_rate = round((emails_with_emoji / len(subjects)) * 100, 1) if subjects else 0
        
        # Date range for frequency
        dates = [e.received_at for e in emails if e.received_at]
        if dates:
            first = min(dates)
            last = max(dates)
            days = (last - first).days or 1
            freq = round((total / days) * 7, 1)
        else:
            freq = 0
        
        if is_authenticated:
            comparison[brand_name] = {
                "total_emails": total,
                "industry": industry,
                "emails_per_week": freq,
                "top_campaign_type": top_campaign,
                "avg_subject_length": avg_length,
                "emoji_usage_rate": emoji_rate
            }
        else:
            comparison[brand_name] = {
                "total_emails": "xx",
                "industry": industry,
                "emails_per_week": "xx",
                "top_campaign_type": top_campaign,
                "avg_subject_length": "xx",
                "emoji_usage_rate": "xx"
            }
    
    return {"comparison": comparison}


@app.get("/analytics/subject-lines")
def get_subject_lines(
    brand: Optional[str] = Query(default=None),
    industry: Optional[str] = Query(default=None),
    campaign_type: Optional[str] = Query(default=None),
    limit: int = Query(default=100, le=500),
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Get subject lines with optional filters.
    Useful for swipe file / inspiration.
    """
    is_authenticated = current_user is not None

    # Require at least Starter plan for analytics
    if current_user:
        plan = get_effective_plan(current_user)
        if plan == "free":
            raise HTTPException(status_code=403, detail="Starter plan or higher required for full analytics. Upgrade at /pricing")

    if not is_authenticated:
        return {
            "message": "Login required to access subject line database",
            "sample": [
                {"subject": "🔥 50% OFF Everything...", "brand": "***", "type": "Sale"},
                {"subject": "Welcome to ***! Here's...", "brand": "***", "type": "Welcome"},
            ],
            "total": "xx"
        }
    
    query = db.query(models.Email)
    
    if brand:
        query = query.filter(models.Email.brand.ilike(brand))
    if industry:
        query = query.filter(models.Email.industry.ilike(industry))
    if campaign_type:
        query = query.filter(models.Email.type.ilike(campaign_type))
    
    query = query.order_by(models.Email.received_at.desc()).limit(limit)
    emails = query.all()
    
    subjects = []
    for e in emails:
        subjects.append({
            "subject": e.subject,
            "brand": e.brand,
            "industry": e.industry,
            "campaign_type": e.type,
            "date": e.received_at.strftime("%Y-%m-%d") if e.received_at else None,
            "length": len(e.subject) if e.subject else 0,
            "has_emoji": bool(_extract_emojis(e.subject)) if e.subject else False
        })
    
    return {
        "total": len(subjects),
        "subjects": subjects
    }


@app.get("/analytics/calendar/{brand_name:path}")
def get_brand_calendar(
    brand_name: str,
    months: int = Query(default=3, le=12, description="Number of months to show"),
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Get campaign calendar/timeline for a brand.
    Shows when they send emails and what types.
    """
    from datetime import datetime, timedelta
    from collections import defaultdict
    
    is_authenticated = current_user is not None

    # Require at least Starter plan for analytics
    if current_user:
        plan = get_effective_plan(current_user)
        if plan == "free":
            raise HTTPException(status_code=403, detail="Starter plan or higher required for full analytics. Upgrade at /pricing")

    # Calculate date range
    end_date = datetime.utcnow()
    start_date = end_date - timedelta(days=months * 30)
    
    emails = db.query(models.Email).filter(
        models.Email.brand.ilike(brand_name),
        models.Email.received_at >= start_date
    ).order_by(models.Email.received_at.desc()).all()
    
    if not emails:
        raise HTTPException(status_code=404, detail=f"No emails found for brand: {brand_name}")
    
    if not is_authenticated:
        return {
            "brand": brand_name,
            "message": "Login to view campaign calendar",
            "total_campaigns": "xx",
            "date_range": {
                "start": start_date.strftime("%Y-%m-%d"),
                "end": end_date.strftime("%Y-%m-%d")
            }
        }
    
    # Group by month
    monthly_data = defaultdict(lambda: {"total": 0, "by_type": defaultdict(int), "emails": []})
    
    for email in emails:
        if email.received_at:
            month_key = email.received_at.strftime("%Y-%m")
            monthly_data[month_key]["total"] += 1
            if email.type:
                monthly_data[month_key]["by_type"][email.type] += 1
            monthly_data[month_key]["emails"].append({
                "date": email.received_at.strftime("%Y-%m-%d"),
                "subject": email.subject,
                "type": email.type
            })
    
    # Convert to list sorted by month
    calendar = []
    for month, data in sorted(monthly_data.items(), reverse=True):
        calendar.append({
            "month": month,
            "total_emails": data["total"],
            "campaign_breakdown": dict(data["by_type"]),
            "emails": data["emails"][:10]  # Limit to 10 per month for brevity
        })
    
    return {
        "brand": brand_name,
        "total_campaigns": len(emails),
        "date_range": {
            "start": start_date.strftime("%Y-%m-%d"),
            "end": end_date.strftime("%Y-%m-%d")
        },
        "calendar": calendar
    }


@app.get("/analytics/overview")
def get_analytics_overview(
    current_user: Optional[models.User] = Depends(get_optional_user),
    db: Session = Depends(get_db)
):
    """
    Get overall platform analytics overview.
    """
    from sqlalchemy import func
    from collections import Counter
    
    is_authenticated = current_user is not None

    # Require at least Starter plan for analytics
    if current_user:
        plan = get_effective_plan(current_user)
        if plan == "free":
            raise HTTPException(status_code=403, detail="Starter plan or higher required for full analytics. Upgrade at /pricing")

    # Total counts
    total_emails = db.query(models.Email).count()
    total_brands = db.query(models.Email.brand).filter(
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).distinct().count()
    
    # Industry breakdown
    industry_counts = db.query(
        models.Email.industry,
        func.count(models.Email.id)
    ).filter(
        models.Email.industry.isnot(None)
    ).group_by(models.Email.industry).all()
    
    industries = {i[0]: i[1] for i in industry_counts}
    
    # Campaign type breakdown
    type_counts = db.query(
        models.Email.type,
        func.count(models.Email.id)
    ).filter(
        models.Email.type.isnot(None)
    ).group_by(models.Email.type).all()
    
    campaign_types = {t[0]: t[1] for t in type_counts}
    
    # Top brands
    brand_counts = db.query(
        models.Email.brand,
        func.count(models.Email.id).label('count')
    ).filter(
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).group_by(models.Email.brand).order_by(func.count(models.Email.id).desc()).limit(10).all()
    
    top_brands = [{"brand": b[0], "count": b[1]} for b in brand_counts]
    
    if is_authenticated:
        return {
            "total_emails": total_emails,
            "total_brands": total_brands,
            "industries": industries,
            "campaign_types": campaign_types,
            "top_brands": top_brands
        }
    else:
        return {
            "total_emails": total_emails,
            "total_brands": total_brands,
            "industries": {k: "xx" for k in industries},
            "campaign_types": {k: "xx" for k in campaign_types},
            "top_brands": [{"brand": b["brand"], "count": "xx"} for b in top_brands]
        }


# ============ SEO Endpoints (Public, Ungated) ============

# Type slug mapping
TYPE_SLUG_MAP = {
    "sale-emails": "Sale",
    "welcome-emails": "Welcome",
    "abandoned-cart-emails": "Abandoned Cart",
    "newsletter-emails": "Newsletter",
    "new-arrival-emails": "New Arrival",
    "re-engagement-emails": "Re-engagement",
    "order-update-emails": "Order Update",
    "festive-emails": "Festive",
    "loyalty-emails": "Loyalty",
    "feedback-emails": "Feedback",
    "back-in-stock-emails": "Back in Stock",
    "educational-emails": "Educational",
    "product-showcase-emails": "Product Showcase",
    "promotional-emails": "Promotional",
    "confirmation-emails": "Confirmation",
}
TYPE_NAME_TO_SLUG = {v: k for k, v in TYPE_SLUG_MAP.items()}

# Festival date ranges for campaign pages
INDIAN_FESTIVALS = {
    "diwali": {
        "name": "Diwali",
        "keywords": ["diwali", "deepavali", "festival of lights"],
        "date_ranges": {
            2024: ("2024-10-15", "2024-11-10"),
            2025: ("2025-10-10", "2025-11-05"),
        },
    },
    "holi": {
        "name": "Holi",
        "keywords": ["holi", "festival of colors", "colour"],
        "date_ranges": {
            2024: ("2024-03-15", "2024-03-30"),
            2025: ("2025-03-05", "2025-03-20"),
        },
    },
    "navratri": {
        "name": "Navratri",
        "keywords": ["navratri", "navaratri", "durga puja", "dandiya", "garba"],
        "date_ranges": {
            2024: ("2024-10-03", "2024-10-15"),
            2025: ("2025-09-22", "2025-10-03"),
        },
    },
    "republic-day-sale": {
        "name": "Republic Day Sale",
        "keywords": ["republic day", "26 january", "26th january", "26jan"],
        "date_ranges": {
            2024: ("2024-01-20", "2024-01-31"),
            2025: ("2025-01-20", "2025-01-31"),
            2026: ("2026-01-20", "2026-01-31"),
        },
    },
    "independence-day-sale": {
        "name": "Independence Day Sale",
        "keywords": ["independence day", "15 august", "15th august", "freedom sale"],
        "date_ranges": {
            2024: ("2024-08-10", "2024-08-20"),
            2025: ("2025-08-10", "2025-08-20"),
        },
    },
    "eoss": {
        "name": "End of Season Sale (EOSS)",
        "keywords": ["eoss", "end of season", "season sale", "clearance"],
        "date_ranges": {
            2024: ("2024-06-15", "2024-07-15"),
            2025: ("2025-01-01", "2025-01-31"),
        },
    },
    "new-year": {
        "name": "New Year",
        "keywords": ["new year", "happy new year", "nye", "new years"],
        "date_ranges": {
            2024: ("2023-12-25", "2024-01-05"),
            2025: ("2024-12-25", "2025-01-05"),
            2026: ("2025-12-25", "2026-01-05"),
        },
    },
    "valentines-day": {
        "name": "Valentine's Day",
        "keywords": ["valentine", "valentines", "love", "cupid"],
        "date_ranges": {
            2024: ("2024-02-07", "2024-02-16"),
            2025: ("2025-02-07", "2025-02-16"),
            2026: ("2026-02-07", "2026-02-16"),
        },
    },
    "rakhi": {
        "name": "Raksha Bandhan",
        "keywords": ["rakhi", "raksha bandhan", "rakshabandhan"],
        "date_ranges": {
            2024: ("2024-08-15", "2024-08-22"),
            2025: ("2025-08-05", "2025-08-12"),
        },
    },
    "christmas": {
        "name": "Christmas",
        "keywords": ["christmas", "xmas", "merry christmas", "santa"],
        "date_ranges": {
            2024: ("2024-12-15", "2024-12-31"),
            2025: ("2025-12-15", "2025-12-31"),
        },
    },
    "womens-day": {
        "name": "Women's Day",
        "keywords": ["women's day", "womens day", "international women"],
        "date_ranges": {
            2024: ("2024-03-01", "2024-03-10"),
            2025: ("2025-03-01", "2025-03-10"),
            2026: ("2026-03-01", "2026-03-10"),
        },
    },
    "mothers-day": {
        "name": "Mother's Day",
        "keywords": ["mother's day", "mothers day", "mom"],
        "date_ranges": {
            2024: ("2024-05-06", "2024-05-14"),
            2025: ("2025-05-05", "2025-05-13"),
        },
    },
    "fathers-day": {
        "name": "Father's Day",
        "keywords": ["father's day", "fathers day", "dad"],
        "date_ranges": {
            2024: ("2024-06-10", "2024-06-18"),
            2025: ("2025-06-09", "2025-06-17"),
        },
    },
}


def _get_brand_seo_data(brand_name: str, db: Session) -> dict:
    """Shared helper to compute brand SEO data (used by brand and compare endpoints)."""
    from collections import Counter
    import re

    emails = db.query(models.Email).filter(
        models.Email.brand.ilike(brand_name)
    ).all()

    if not emails:
        return None

    total_emails = len(emails)

    # Industry
    industries = Counter(e.industry for e in emails if e.industry)
    primary_industry = industries.most_common(1)[0][0] if industries else None

    # Campaign breakdown
    campaign_types = Counter(e.type for e in emails if e.type)
    campaign_breakdown = {k: v for k, v in campaign_types.most_common()}

    # Send day + time distribution
    day_distribution = Counter()
    time_distribution = Counter()
    for email in emails:
        if email.received_at:
            day_distribution[_get_day_name(email.received_at.weekday())] += 1
            time_distribution[_get_time_bucket(email.received_at.hour)] += 1

    # Subject line analysis
    subjects = [e.subject for e in emails if e.subject]
    avg_subject_length = round(sum(len(s) for s in subjects) / len(subjects), 1) if subjects else 0
    emails_with_emoji = sum(1 for s in subjects if _extract_emojis(s))
    emoji_rate = round((emails_with_emoji / len(subjects)) * 100, 1) if subjects else 0

    stop_words = {'the', 'a', 'an', 'is', 'are', 'and', 'or', 'to', 'for', 'of', 'in', 'on', 'at', 'your', 'you', 'we', 'our', 'this', 'that', 'it', 'with'}
    all_words = []
    for s in subjects:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', s.lower())
        all_words.extend(w for w in words if w not in stop_words)
    top_words = [word for word, _ in Counter(all_words).most_common(10)]

    # Sample subjects (5 most recent unique)
    sample_subjects = []
    seen = set()
    for e in sorted(emails, key=lambda x: x.received_at or datetime.min, reverse=True):
        if e.subject and e.subject not in seen:
            sample_subjects.append(e.subject)
            seen.add(e.subject)
        if len(sample_subjects) >= 5:
            break

    # Date range + frequency
    dates = [e.received_at for e in emails if e.received_at]
    first_email = min(dates) if dates else None
    last_email = max(dates) if dates else None
    if first_email and last_email and first_email != last_email:
        days = (last_email - first_email).days
        emails_per_week = round((total_emails / days) * 7, 1) if days > 0 else total_emails
    else:
        emails_per_week = total_emails

    # Recent emails (10, lightweight)
    recent = sorted(emails, key=lambda x: x.received_at or datetime.min, reverse=True)[:10]
    recent_emails = [
        {"id": e.id, "subject": e.subject, "type": e.type, "received_at": e.received_at.isoformat() if e.received_at else None}
        for e in recent
    ]

    # Festive campaigns
    festive_campaigns = []
    for fest_slug, fest_data in INDIAN_FESTIVALS.items():
        for year, (start_str, end_str) in fest_data["date_ranges"].items():
            start = datetime.fromisoformat(start_str)
            end = datetime.fromisoformat(end_str)
            count = sum(
                1 for e in emails
                if e.received_at and start <= e.received_at <= end
                and (
                    (e.type and e.type.lower() in ("festive", "sale"))
                    or any(kw in (e.subject or "").lower() for kw in fest_data["keywords"])
                )
            )
            if count > 0:
                festive_campaigns.append({"festival": fest_data["name"], "count": count, "year": year})

    # Related brands (same industry)
    related_brands = []
    if primary_industry:
        from sqlalchemy import func
        related_rows = db.query(models.Email.brand).filter(
            models.Email.industry.ilike(primary_industry),
            models.Email.brand.isnot(None),
            models.Email.brand != "Unknown",
            ~models.Email.brand.ilike(brand_name)
        ).distinct().all()
        related_brands = sorted([r[0] for r in related_rows if r[0]])[:12]

    return {
        "brand": brand_name,
        "industry": primary_industry,
        "total_emails": total_emails,
        "emails_per_week": emails_per_week,
        "first_email": first_email.isoformat() if first_email else None,
        "last_email": last_email.isoformat() if last_email else None,
        "campaign_breakdown": campaign_breakdown,
        "send_day_distribution": dict(day_distribution),
        "send_time_distribution": dict(time_distribution),
        "subject_line_stats": {
            "avg_length": avg_subject_length,
            "emoji_usage_rate": emoji_rate,
            "top_words": top_words,
            "sample_subjects": sample_subjects,
        },
        "recent_emails": recent_emails,
        "festive_campaigns": festive_campaigns,
        "related_brands": related_brands,
    }


@app.get("/seo/brand/{brand_name:path}")
def seo_brand(brand_name: str, db: Session = Depends(get_db)):
    """Public, ungated brand data for SEO pages. No auth required."""
    data = _get_brand_seo_data(brand_name, db)
    if not data:
        raise HTTPException(status_code=404, detail=f"No emails found for brand: {brand_name}")
    return data


@app.get("/seo/industry/{industry}")
def seo_industry(industry: str, db: Session = Depends(get_db)):
    """Public, ungated industry data for SEO pages. Superset of /industries/{industry}/seo."""
    from sqlalchemy import func
    from collections import Counter

    emails = db.query(models.Email).filter(
        models.Email.industry.ilike(industry)
    ).all()

    if not emails:
        raise HTTPException(status_code=404, detail=f"No emails found for industry: {industry}")

    total_emails = len(emails)

    # Brand breakdown
    brand_counts = Counter(e.brand for e in emails if e.brand and e.brand != "Unknown")
    brands = sorted(brand_counts.keys())
    total_brands = len(brand_counts)
    top_brands = [
        {"brand": b, "email_count": c, "emails_per_week": 0}
        for b, c in brand_counts.most_common(10)
    ]
    # Compute per-week for top brands
    for tb in top_brands:
        brand_emails = [e for e in emails if e.brand == tb["brand"]]
        dates = [e.received_at for e in brand_emails if e.received_at]
        if len(dates) >= 2:
            days = (max(dates) - min(dates)).days
            if days > 0:
                tb["emails_per_week"] = round((len(brand_emails) / days) * 7, 1)

    # Campaign type breakdown
    campaign_types = Counter(e.type for e in emails if e.type)
    top_campaign_types = [
        {"type": t, "count": c, "percentage": round((c / total_emails) * 100, 1)}
        for t, c in campaign_types.most_common()
    ]

    # Recent emails
    sorted_emails = sorted(emails, key=lambda x: x.received_at or datetime.min, reverse=True)
    recent_emails = [
        {"id": e.id, "subject": e.subject, "brand": e.brand, "type": e.type, "received_at": e.received_at.isoformat() if e.received_at else None}
        for e in sorted_emails[:15]
    ]

    # Subject + send stats
    subjects = [e.subject for e in emails if e.subject]
    avg_subject_length = round(sum(len(s) for s in subjects) / len(subjects), 1) if subjects else 0
    emoji_count = sum(1 for s in subjects if _extract_emojis(s))
    emoji_usage_rate = round((emoji_count / len(subjects)) * 100, 1) if subjects else 0

    day_distribution = Counter()
    time_distribution = Counter()
    for e in emails:
        if e.received_at:
            day_distribution[_get_day_name(e.received_at.weekday())] += 1
            time_distribution[_get_time_bucket(e.received_at.hour)] += 1

    peak_send_day = day_distribution.most_common(1)[0][0] if day_distribution else None
    peak_send_time = time_distribution.most_common(1)[0][0] if time_distribution else None

    # Per-brand frequency average
    brand_email_counts = list(brand_counts.values())
    # Calculate average emails per brand per week across the industry
    all_dates = [e.received_at for e in emails if e.received_at]
    if all_dates and len(all_dates) >= 2:
        total_days = (max(all_dates) - min(all_dates)).days
        if total_days > 0 and total_brands > 0:
            avg_emails_per_brand_per_week = round(((total_emails / total_brands) / total_days) * 7, 1)
        else:
            avg_emails_per_brand_per_week = 0
    else:
        avg_emails_per_brand_per_week = 0

    # Seasonal activity (last 12 months)
    seasonal_activity = Counter()
    for e in emails:
        if e.received_at:
            seasonal_activity[e.received_at.strftime("%Y-%m")] += 1
    seasonal_list = [{"month": m, "count": c} for m, c in sorted(seasonal_activity.items())[-12:]]

    return {
        "industry": industry,
        "total_brands": total_brands,
        "total_emails": total_emails,
        "brands": brands,
        "top_brands": top_brands,
        "top_campaign_types": top_campaign_types,
        "recent_emails": recent_emails,
        "avg_emails_per_brand_per_week": avg_emails_per_brand_per_week,
        "avg_subject_length": avg_subject_length,
        "emoji_usage_rate": emoji_usage_rate,
        "peak_send_day": peak_send_day,
        "peak_send_time": peak_send_time,
        "seasonal_activity": seasonal_list,
    }


@app.get("/seo/types")
def seo_types_list(db: Session = Depends(get_db)):
    """List all email types with slugs and counts."""
    from sqlalchemy import func

    type_counts = db.query(
        models.Email.type,
        func.count(models.Email.id)
    ).filter(
        models.Email.type.isnot(None)
    ).group_by(models.Email.type).all()

    results = []
    for type_name, count in type_counts:
        slug = TYPE_NAME_TO_SLUG.get(type_name)
        if slug:
            results.append({"type": type_name, "slug": slug, "count": count})
    return sorted(results, key=lambda x: x["count"], reverse=True)


@app.get("/seo/types/{type_slug}")
def seo_type_detail(type_slug: str, db: Session = Depends(get_db)):
    """Public data for email type SEO pages."""
    from sqlalchemy import func
    from collections import Counter
    import re

    type_name = TYPE_SLUG_MAP.get(type_slug)
    if not type_name:
        raise HTTPException(status_code=404, detail=f"Unknown type slug: {type_slug}")

    emails = db.query(models.Email).filter(
        models.Email.type.ilike(type_name)
    ).all()

    if not emails:
        raise HTTPException(status_code=404, detail=f"No emails found for type: {type_name}")

    total_emails = len(emails)

    # Brands
    brand_counts = Counter(e.brand for e in emails if e.brand and e.brand != "Unknown")
    total_brands = len(brand_counts)
    top_brands = [{"brand": b, "count": c} for b, c in brand_counts.most_common(10)]

    # Industry breakdown
    industry_counts = Counter(e.industry for e in emails if e.industry)
    industry_breakdown = [
        {"industry": ind, "count": c, "percentage": round((c / total_emails) * 100, 1)}
        for ind, c in industry_counts.most_common()
    ]

    # Example emails (15, brand-diverse: max 2 per brand)
    sorted_emails = sorted(emails, key=lambda x: x.received_at or datetime.min, reverse=True)
    example_emails = []
    brand_seen = Counter()
    for e in sorted_emails:
        if brand_seen[e.brand] < 2:
            example_emails.append({
                "id": e.id, "subject": e.subject, "brand": e.brand,
                "industry": e.industry,
                "received_at": e.received_at.isoformat() if e.received_at else None,
            })
            brand_seen[e.brand] += 1
        if len(example_emails) >= 15:
            break

    # Subject stats
    subjects = [e.subject for e in emails if e.subject]
    avg_subject_length = round(sum(len(s) for s in subjects) / len(subjects), 1) if subjects else 0
    emoji_count = sum(1 for s in subjects if _extract_emojis(s))
    emoji_usage_rate = round((emoji_count / len(subjects)) * 100, 1) if subjects else 0

    stop_words = {'the', 'a', 'an', 'is', 'are', 'and', 'or', 'to', 'for', 'of', 'in', 'on', 'at', 'your', 'you', 'we', 'our', 'this', 'that', 'it', 'with'}
    all_words = []
    for s in subjects:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', s.lower())
        all_words.extend(w for w in words if w not in stop_words)
    top_subject_words = [word for word, _ in Counter(all_words).most_common(10)]

    sample_subjects = list({e.subject for e in sorted_emails if e.subject})[:10]

    # Peak send day
    day_dist = Counter()
    for e in emails:
        if e.received_at:
            day_dist[_get_day_name(e.received_at.weekday())] += 1
    peak_send_day = day_dist.most_common(1)[0][0] if day_dist else None

    # Related types (exclude self, pick types that co-occur with same brands)
    related_types = [slug for slug in TYPE_SLUG_MAP if slug != type_slug][:5]

    return {
        "type": type_name,
        "slug": type_slug,
        "total_emails": total_emails,
        "total_brands": total_brands,
        "example_emails": example_emails,
        "top_brands": top_brands,
        "industry_breakdown": industry_breakdown,
        "avg_subject_length": avg_subject_length,
        "emoji_usage_rate": emoji_usage_rate,
        "top_subject_words": top_subject_words,
        "sample_subjects": sample_subjects,
        "peak_send_day": peak_send_day,
        "related_types": related_types,
    }


@app.get("/seo/email/{email_id}")
def seo_email(email_id: int, db: Session = Depends(get_db)):
    """Public email data for SEO pages. No HTML (loaded client-side)."""
    email = db.query(models.Email).filter(models.Email.id == email_id).first()
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")

    # Subject analysis
    subject = email.subject or ""
    import re
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U000024C2-\U0001F251]+",
        flags=re.UNICODE
    )
    urgency_words = {"limited", "hurry", "last chance", "ending", "expires", "final", "only", "rush", "midnight", "tonight", "today only"}
    subject_lower = subject.lower()

    analysis = {
        "char_count": len(subject),
        "word_count": len(subject.split()),
        "has_emoji": bool(emoji_pattern.search(subject)),
        "has_question": "?" in subject,
        "has_number": bool(re.search(r'\d', subject)),
        "has_personalization": any(tok in subject_lower for tok in ["{first_name}", "{name}", "{{name}}", "hi ,", "hey ,"]),
        "has_urgency": any(w in subject_lower for w in urgency_words),
    }

    # More from brand (5)
    more_from_brand = []
    if email.brand:
        related = db.query(models.Email).filter(
            models.Email.brand.ilike(email.brand),
            models.Email.id != email_id,
        ).order_by(models.Email.received_at.desc()).limit(5).all()
        more_from_brand = [
            {"id": e.id, "subject": e.subject, "type": e.type, "received_at": e.received_at.isoformat() if e.received_at else None}
            for e in related
        ]

    # Similar emails (same type + industry, 5)
    similar_emails = []
    if email.type and email.industry:
        similar = db.query(models.Email).filter(
            models.Email.type == email.type,
            models.Email.industry == email.industry,
            models.Email.id != email_id,
        ).order_by(models.Email.received_at.desc()).limit(5).all()
        similar_emails = [
            {"id": e.id, "subject": e.subject, "brand": e.brand, "type": e.type, "received_at": e.received_at.isoformat() if e.received_at else None}
            for e in similar
        ]

    return {
        "id": email.id,
        "subject": email.subject,
        "brand": email.brand,
        "sender": email.sender,
        "type": email.type,
        "industry": email.industry,
        "received_at": email.received_at.isoformat() if email.received_at else None,
        "preview": email.preview,
        "analysis": analysis,
        "more_from_brand": more_from_brand,
        "similar_emails": similar_emails,
    }


@app.get("/seo/compare/pairs")
def seo_compare_pairs(db: Session = Depends(get_db)):
    """Get all brand comparison pairs for sitemap/static generation.
    Top 5 brands per industry, all C(5,2) = 10 pairs per industry."""
    from sqlalchemy import func
    from itertools import combinations

    # Get top 5 brands per industry by email count
    industries = db.query(models.Email.industry).filter(
        models.Email.industry.isnot(None)
    ).distinct().all()

    pairs = []
    for (industry,) in industries:
        top_brands = db.query(
            models.Email.brand,
            func.count(models.Email.id).label("cnt")
        ).filter(
            models.Email.industry.ilike(industry),
            models.Email.brand.isnot(None),
            models.Email.brand != "Unknown",
        ).group_by(models.Email.brand).order_by(func.count(models.Email.id).desc()).limit(5).all()

        brand_names = [b[0] for b in top_brands if b[1] >= 20]  # Min 20 emails
        for a, b in combinations(brand_names, 2):
            sorted_pair = sorted([a, b])
            pairs.append({"brand_a": sorted_pair[0], "brand_b": sorted_pair[1], "industry": industry})

    return pairs


@app.get("/seo/compare/{brand_a}/{brand_b}")
def seo_compare(brand_a: str, brand_b: str, db: Session = Depends(get_db)):
    """Public comparison data for two brands."""
    data_a = _get_brand_seo_data(brand_a, db)
    data_b = _get_brand_seo_data(brand_b, db)

    if not data_a:
        raise HTTPException(status_code=404, detail=f"No emails found for brand: {brand_a}")
    if not data_b:
        raise HTTPException(status_code=404, detail=f"No emails found for brand: {brand_b}")

    # Build comparison summary
    summary = {}
    summary["more_active"] = data_a["brand"] if data_a["emails_per_week"] >= data_b["emails_per_week"] else data_b["brand"]

    a_len = data_a["subject_line_stats"]["avg_length"]
    b_len = data_b["subject_line_stats"]["avg_length"]
    summary["longer_subjects"] = data_a["brand"] if a_len >= b_len else data_b["brand"]

    a_emoji = data_a["subject_line_stats"]["emoji_usage_rate"]
    b_emoji = data_b["subject_line_stats"]["emoji_usage_rate"]
    summary["more_emoji"] = data_a["brand"] if a_emoji >= b_emoji else data_b["brand"]

    shared_industry = None
    if data_a["industry"] and data_b["industry"] and data_a["industry"].lower() == data_b["industry"].lower():
        shared_industry = data_a["industry"]

    return {
        "brand_a": data_a,
        "brand_b": data_b,
        "shared_industry": shared_industry,
        "comparison_summary": summary,
    }


@app.get("/seo/campaigns")
def seo_campaigns_list(db: Session = Depends(get_db)):
    """List all available festival campaign pages with email counts."""
    results = []
    for fest_slug, fest_data in INDIAN_FESTIVALS.items():
        for year, (start_str, end_str) in fest_data["date_ranges"].items():
            start = datetime.fromisoformat(start_str)
            end = datetime.fromisoformat(end_str)
            count = db.query(models.Email).filter(
                models.Email.received_at >= start,
                models.Email.received_at <= end,
            ).count()
            if count >= 5:  # Only include if at least 5 emails
                results.append({
                    "festival": fest_data["name"],
                    "slug": fest_slug,
                    "year": year,
                    "count": count,
                })
    return sorted(results, key=lambda x: (x["year"], x["festival"]), reverse=True)


@app.get("/seo/campaigns/{festival_slug}/{year}")
def seo_campaign_detail(festival_slug: str, year: int, db: Session = Depends(get_db)):
    """Public data for festival/seasonal campaign SEO pages."""
    from collections import Counter

    fest_data = INDIAN_FESTIVALS.get(festival_slug)
    if not fest_data:
        raise HTTPException(status_code=404, detail=f"Unknown festival: {festival_slug}")

    date_range = fest_data["date_ranges"].get(year)
    if not date_range:
        raise HTTPException(status_code=404, detail=f"No data for {fest_data['name']} {year}")

    start = datetime.fromisoformat(date_range[0])
    end = datetime.fromisoformat(date_range[1])

    # Get emails in the date range that are relevant to this festival
    all_in_range = db.query(models.Email).filter(
        models.Email.received_at >= start,
        models.Email.received_at <= end,
    ).all()

    # Filter to festive/sale types or emails with festival keywords in subject
    keywords = fest_data["keywords"]
    emails = [
        e for e in all_in_range
        if (e.type and e.type.lower() in ("festive", "sale"))
        or any(kw in (e.subject or "").lower() for kw in keywords)
    ]

    # If keyword filter is too aggressive, fall back to all emails in range
    if len(emails) < 5:
        emails = all_in_range

    if not emails:
        raise HTTPException(status_code=404, detail=f"No campaign data for {fest_data['name']} {year}")

    total_emails = len(emails)

    # Industry breakdown
    industry_counts = Counter(e.industry for e in emails if e.industry)
    industry_breakdown = [
        {"industry": ind, "count": c, "percentage": round((c / total_emails) * 100, 1)}
        for ind, c in industry_counts.most_common()
    ]

    # Top brands
    brand_counts = Counter(e.brand for e in emails if e.brand and e.brand != "Unknown")
    total_brands = len(brand_counts)
    top_brands = [{"brand": b, "count": c} for b, c in brand_counts.most_common(10)]

    # Campaign type breakdown
    type_counts = Counter(e.type for e in emails if e.type)
    campaign_types = [{"type": t, "count": c} for t, c in type_counts.most_common()]

    # Best subject lines (15, diverse brands)
    sorted_emails = sorted(emails, key=lambda x: x.received_at or datetime.min, reverse=True)
    best_subject_lines = []
    brand_seen = Counter()
    for e in sorted_emails:
        if e.subject and brand_seen[e.brand] < 2:
            best_subject_lines.append({"subject": e.subject, "brand": e.brand, "id": e.id})
            brand_seen[e.brand] += 1
        if len(best_subject_lines) >= 15:
            break

    # Best emails (10)
    best_emails = [
        {"id": e.id, "subject": e.subject, "brand": e.brand, "type": e.type, "received_at": e.received_at.isoformat() if e.received_at else None}
        for e in sorted_emails[:10]
    ]

    # Insights
    subjects = [e.subject for e in emails if e.subject]
    avg_subject_length = round(sum(len(s) for s in subjects) / len(subjects), 1) if subjects else 0
    emoji_count = sum(1 for s in subjects if _extract_emojis(s))
    emoji_usage_rate = round((emoji_count / len(subjects)) * 100, 1) if subjects else 0

    day_dist = Counter()
    for e in emails:
        if e.received_at:
            day_dist[_get_day_name(e.received_at.weekday())] += 1
    peak_send_day = day_dist.most_common(1)[0][0] if day_dist else None

    import re
    stop_words = {'the', 'a', 'an', 'is', 'are', 'and', 'or', 'to', 'for', 'of', 'in', 'on', 'at', 'your', 'you', 'we', 'our', 'this', 'that', 'it', 'with'}
    all_words = []
    for s in subjects:
        words = re.findall(r'\b[a-zA-Z]{3,}\b', s.lower())
        all_words.extend(w for w in words if w not in stop_words)
    top_subject_words = [word for word, _ in Counter(all_words).most_common(10)]

    return {
        "festival": fest_data["name"],
        "slug": festival_slug,
        "year": year,
        "date_range": {"start": date_range[0], "end": date_range[1]},
        "total_emails": total_emails,
        "total_brands": total_brands,
        "industry_breakdown": industry_breakdown,
        "top_brands": top_brands,
        "campaign_types": campaign_types,
        "best_subject_lines": best_subject_lines,
        "best_emails": best_emails,
        "insights": {
            "avg_subject_length": avg_subject_length,
            "emoji_usage_rate": emoji_usage_rate,
            "peak_send_day": peak_send_day,
            "top_subject_words": top_subject_words,
        },
    }


# ── Admin: Tweet Queue ──

@app.get("/admin/tweets")
def list_tweets(
    status: str = None,
    limit: int = 50,
    admin: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    q = db.query(models.TweetQueue).order_by(models.TweetQueue.created_at.desc())
    if status:
        q = q.filter(models.TweetQueue.status == status)
    tweets = q.limit(limit).all()
    return [
        {
            "id": t.id,
            "content": t.content,
            "tweet_type": t.tweet_type,
            "status": t.status,
            "scheduled_for": t.scheduled_for.isoformat() if t.scheduled_for else None,
            "posted_at": t.posted_at.isoformat() if t.posted_at else None,
            "twitter_id": t.twitter_id,
            "created_at": t.created_at.isoformat() if t.created_at else None,
            "char_count": len(t.content),
        }
        for t in tweets
    ]


@app.post("/admin/tweets/generate")
def generate_tweets(
    tweet_type: str = "daily_digest",
    admin: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    valid_types = ["daily_digest", "weekly_digest", "brand_spotlight", "subject_line_insight"]
    if tweet_type not in valid_types:
        raise HTTPException(400, f"Invalid type. Must be one of: {valid_types}")

    content = generate_tweet_content(tweet_type, db)
    tweet = models.TweetQueue(
        content=content,
        tweet_type=tweet_type,
        status="draft",
    )
    db.add(tweet)
    db.commit()
    db.refresh(tweet)
    return {
        "id": tweet.id,
        "content": tweet.content,
        "tweet_type": tweet.tweet_type,
        "status": tweet.status,
        "char_count": len(tweet.content),
    }


@app.patch("/admin/tweets/{tweet_id}/approve")
def approve_tweet(
    tweet_id: int,
    admin: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    tweet = db.query(models.TweetQueue).filter(models.TweetQueue.id == tweet_id).first()
    if not tweet:
        raise HTTPException(404, "Tweet not found")
    tweet.status = "approved"
    tweet.updated_at = datetime.utcnow()
    db.commit()
    return {"id": tweet.id, "status": "approved"}


@app.patch("/admin/tweets/{tweet_id}/reject")
def reject_tweet(
    tweet_id: int,
    admin: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    tweet = db.query(models.TweetQueue).filter(models.TweetQueue.id == tweet_id).first()
    if not tweet:
        raise HTTPException(404, "Tweet not found")
    tweet.status = "rejected"
    tweet.updated_at = datetime.utcnow()
    db.commit()
    return {"id": tweet.id, "status": "rejected"}


@app.patch("/admin/tweets/{tweet_id}/edit")
def edit_tweet(
    tweet_id: int,
    body: dict,
    admin: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    tweet = db.query(models.TweetQueue).filter(models.TweetQueue.id == tweet_id).first()
    if not tweet:
        raise HTTPException(404, "Tweet not found")
    new_content = body.get("content", "").strip()
    if not new_content:
        raise HTTPException(400, "Content cannot be empty")
    if len(new_content) > 280:
        raise HTTPException(400, f"Tweet too long ({len(new_content)}/280 chars)")
    tweet.content = new_content
    tweet.updated_at = datetime.utcnow()
    db.commit()
    return {"id": tweet.id, "content": tweet.content, "char_count": len(tweet.content)}


@app.post("/admin/tweets/{tweet_id}/post")
def post_tweet_endpoint(
    tweet_id: int,
    admin: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    tweet = db.query(models.TweetQueue).filter(models.TweetQueue.id == tweet_id).first()
    if not tweet:
        raise HTTPException(404, "Tweet not found")
    if tweet.status == "posted":
        raise HTTPException(400, "Tweet already posted")
    if not is_twitter_configured():
        raise HTTPException(400, "Twitter API not configured. Set TWITTER_API_KEY, TWITTER_API_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET env vars.")

    twitter_id = post_tweet(tweet.content)
    tweet.status = "posted"
    tweet.posted_at = datetime.utcnow()
    tweet.twitter_id = twitter_id
    tweet.updated_at = datetime.utcnow()
    db.commit()
    return {"id": tweet.id, "status": "posted", "twitter_id": twitter_id}


@app.delete("/admin/tweets/{tweet_id}")
def delete_tweet(
    tweet_id: int,
    admin: models.User = Depends(get_admin_user),
    db: Session = Depends(get_db),
):
    tweet = db.query(models.TweetQueue).filter(models.TweetQueue.id == tweet_id).first()
    if not tweet:
        raise HTTPException(404, "Tweet not found")
    db.delete(tweet)
    db.commit()
    return {"deleted": True}
