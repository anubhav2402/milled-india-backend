from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from sqlalchemy import text

from . import models, schemas
from .db import Base, SessionLocal, engine
from .utils import extract_preview_image_url

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
        # Allow any Vercel preview deployments
        "https://*.vercel.app",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
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


@app.get("/emails", response_model=List[schemas.EmailOut])
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
    Get emails, sorted by newest first.
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
    
    emails = query.all()
    
    # Add preview_image_url to each email
    result = []
    for email in emails:
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
        result.append(schemas.EmailOut(**email_dict))
    
    return result


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
