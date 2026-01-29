"""
Script to update brand names for existing emails using improved extraction logic.
Run this to fix brand names for emails already in the database.
"""
from sqlalchemy.orm import Session
from backend.db import SessionLocal, engine, Base
from backend.models import Email
from engine import extract_brand

def update_brands():
    """Update brand names for all emails in the database."""
    Base.metadata.create_all(bind=engine)
    
    db = SessionLocal()
    try:
        emails = db.query(Email).all()
        updated = 0
        unchanged = 0
        
        print(f"Found {len(emails)} emails to process...")
        
        for email in emails:
            # Re-extract brand with improved logic (including HTML)
            new_brand = extract_brand(email.sender, email.html)
            
            if new_brand != email.brand:
                old_brand = email.brand
                email.brand = new_brand
                updated += 1
                print(f"Updated email {email.id}: '{old_brand}' -> '{new_brand}'")
            else:
                unchanged += 1
        
        db.commit()
        print(f"\nUpdate complete. Updated: {updated}, Unchanged: {unchanged}")
        
    finally:
        db.close()


if __name__ == "__main__":
    update_brands()
