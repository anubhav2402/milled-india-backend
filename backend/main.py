from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text

from . import models, schemas
from .db import Base, SessionLocal, engine
from .utils import extract_preview_image_url
from .auth import (
    hash_password,
    verify_password,
    create_access_token,
    get_current_user,
    get_optional_user,
    verify_google_token,
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

run_migrations()

app = FastAPI(title="Milled India API", version="0.1.0")

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
def create_tables():
    """Create all database tables. Use this to ensure user tables exist."""
    try:
        Base.metadata.create_all(bind=engine)
        return {"message": "Tables created successfully", "tables": list(Base.metadata.tables.keys())}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to create tables: {str(e)}")


# ============ Authentication Endpoints ============

@app.post("/auth/register", response_model=schemas.TokenResponse)
def register(user_data: schemas.UserCreate, db: Session = Depends(get_db)):
    """Register a new user with email and password."""
    try:
        # Check if email already exists
        existing_user = db.query(models.User).filter(models.User.email == user_data.email).first()
        if existing_user:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Email already registered"
            )
        
        # Create new user
        user = models.User(
            email=user_data.email,
            password_hash=hash_password(user_data.password),
            name=user_data.name,
        )
        db.add(user)
        db.commit()
        db.refresh(user)
        
        # Create access token
        token = create_access_token(user.id, user.email)
        
        return schemas.TokenResponse(
            access_token=token,
            user=schemas.UserOut(id=user.id, email=user.email, name=user.name)
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
def login(credentials: schemas.UserLogin, db: Session = Depends(get_db)):
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
        user=schemas.UserOut(id=user.id, email=user.email, name=user.name)
    )


@app.post("/auth/google", response_model=schemas.TokenResponse)
def google_auth(auth_data: schemas.GoogleAuth, db: Session = Depends(get_db)):
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
            # Create new user
            user = models.User(
                email=google_info["email"],
                google_id=google_info["google_id"],
                name=google_info.get("name"),
            )
            db.add(user)
        
        db.commit()
        db.refresh(user)
    
    # Create access token
    token = create_access_token(user.id, user.email)
    
    return schemas.TokenResponse(
        access_token=token,
        user=schemas.UserOut(id=user.id, email=user.email, name=user.name)
    )


@app.get("/auth/me", response_model=schemas.UserOut)
def get_me(current_user: models.User = Depends(get_current_user)):
    """Get current authenticated user info."""
    return schemas.UserOut(
        id=current_user.id,
        email=current_user.email,
        name=current_user.name
    )


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
    """Follow a brand."""
    # Check if already following
    existing = db.query(models.UserFollow).filter(
        models.UserFollow.user_id == current_user.id,
        models.UserFollow.brand_name == brand_name
    ).first()
    
    if existing:
        return {"message": "Already following this brand"}
    
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
    """Bookmark an email."""
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


# ============ Email Endpoints ============

@app.get("/emails", response_model=List[schemas.EmailListOut])
def list_emails(
    brand: Optional[str] = Query(default=None),
    type: Optional[str] = Query(default=None),
    industry: Optional[str] = Query(default=None),
    q: Optional[str] = Query(default=None),
    skip: int = 0,
    limit: Optional[int] = Query(default=None),  # No limit by default - returns all
    db: Session = Depends(get_db),
):
    """
    Get emails (lightweight - no HTML), sorted by newest first.
    If no limit is specified, returns all emails.
    """
    query = db.query(models.Email).order_by(models.Email.received_at.desc())

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
def get_emails_html(
    ids: List[int],
    db: Session = Depends(get_db),
):
    """
    Get HTML content for specific email IDs (for lazy loading previews).
    Returns a dict mapping email_id -> html content.
    """
    if len(ids) > 50:
        ids = ids[:50]  # Limit to 50 at a time
    
    emails = db.query(models.Email).filter(models.Email.id.in_(ids)).all()
    return {email.id: email.html for email in emails}


@app.get("/emails/{email_id}/html")
def get_email_html(email_id: int, db: Session = Depends(get_db)):
    """Get just the HTML content for a single email (for lazy loading preview)."""
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


@app.get("/brands", response_model=List[str])
def list_brands(db: Session = Depends(get_db)):
    """Get list of all unique brands."""
    result = db.query(models.Email.brand).filter(
        models.Email.brand.isnot(None),
        models.Email.brand != "Unknown"
    ).distinct().all()
    return sorted([r[0] for r in result if r[0]])


@app.get("/emails/{email_id}", response_model=schemas.EmailOut)
def get_email(email_id: int, db: Session = Depends(get_db)):
    email = db.query(models.Email).filter(models.Email.id == email_id).first()
    if not email:
        raise HTTPException(status_code=404, detail="Email not found")
    
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
        "html": email.html,
        "preview_image_url": extract_preview_image_url(email.html),
    }
    return schemas.EmailOut(**email_dict)


@app.post("/admin/update-industries")
def update_industries(db: Session = Depends(get_db)):
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
def get_brands_without_industry(db: Session = Depends(get_db)):
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
def update_brands(db: Session = Depends(get_db)):
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
def reprocess_all(db: Session = Depends(get_db)):
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
def clear_all_emails(db: Session = Depends(get_db)):
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
def update_campaign_types(db: Session = Depends(get_db)):
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
def test_ai_classification():
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
    Get statistics for all brands including send frequency.
    Returns email count and average emails per week for each brand.
    If not authenticated, stats are masked with "xx".
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
        
        # Mask stats for non-authenticated users
        if is_authenticated:
            brand_stats[brand] = {
                "email_count": count,
                "send_frequency": freq
            }
        else:
            brand_stats[brand] = {
                "email_count": "xx",
                "send_frequency": "xx"
            }
    
    return brand_stats


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


@app.get("/analytics/brand/{brand_name}")
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
    if is_authenticated:
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


@app.get("/analytics/calendar/{brand_name}")
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
