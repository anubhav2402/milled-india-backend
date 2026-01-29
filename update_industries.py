#!/usr/bin/env python3
"""
Update existing emails with industry classification based on brand name.
"""
from sqlalchemy.orm import Session
from backend.db import SessionLocal, engine
from backend.models import Base, Email
from engine import extract_industry


def main():
    Base.metadata.create_all(bind=engine)
    
    db: Session = SessionLocal()
    
    try:
        # Get all emails
        emails = db.query(Email).all()
        total = len(emails)
        print(f"Found {total} emails to process\n")
        
        updated = 0
        already_set = 0
        no_match = 0
        
        for i, email in enumerate(emails, 1):
            if email.industry:
                already_set += 1
                continue
            
            industry = extract_industry(email.brand)
            
            if industry:
                email.industry = industry
                updated += 1
                print(f"[{i}/{total}] {email.brand} -> {industry}")
            else:
                no_match += 1
                if email.brand:
                    print(f"[{i}/{total}] No match for: {email.brand}")
        
        db.commit()
        
        print(f"\n{'='*50}")
        print(f"Industry update complete!")
        print(f"  Updated:     {updated}")
        print(f"  Already set: {already_set}")
        print(f"  No match:    {no_match}")
        
    finally:
        db.close()


if __name__ == "__main__":
    main()
