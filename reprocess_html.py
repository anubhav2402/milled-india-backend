#!/usr/bin/env python3
"""
Reprocess existing emails with updated HTML sanitization.
This script fetches the original HTML from Gmail and re-sanitizes it
with the improved clean_html function that preserves more formatting.
"""
import os
from sqlalchemy.orm import Session
from backend.db import SessionLocal, engine
from backend.models import Base, Email
from engine import authenticate, clean_html
from googleapiclient.discovery import build
import base64


def get_original_html(service, gmail_id: str) -> str | None:
    """Fetch the original HTML content from Gmail."""
    try:
        msg = service.users().messages().get(
            userId='me',
            id=gmail_id,
            format='full'
        ).execute()
        
        payload = msg.get('payload', {})
        
        # Case 1: Simple HTML email
        if payload.get("mimeType") == "text/html":
            data = payload.get("body", {}).get("data")
            if data:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
        
        # Case 2: Multipart - find HTML part
        def find_html(parts):
            for part in parts:
                mime = part.get("mimeType", "")
                if mime == "text/html":
                    data = part.get("body", {}).get("data")
                    if data:
                        return base64.urlsafe_b64decode(data).decode("utf-8", errors="ignore")
                if "parts" in part:
                    result = find_html(part["parts"])
                    if result:
                        return result
            return None
        
        if "parts" in payload:
            return find_html(payload["parts"])
        
        return None
    except Exception as e:
        print(f"  Error fetching {gmail_id}: {e}")
        return None


def main():
    Base.metadata.create_all(bind=engine)
    
    print("Authenticating with Gmail...")
    creds = authenticate()
    service = build('gmail', 'v1', credentials=creds)
    
    db: Session = SessionLocal()
    
    try:
        emails = db.query(Email).all()
        total = len(emails)
        print(f"Found {total} emails to reprocess\n")
        
        updated = 0
        failed = 0
        
        for i, email in enumerate(emails, 1):
            print(f"[{i}/{total}] Processing: {email.subject[:50]}...")
            
            if not email.gmail_id:
                print("  Skipped: No Gmail ID")
                failed += 1
                continue
            
            original_html = get_original_html(service, email.gmail_id)
            
            if not original_html:
                print("  Skipped: Could not fetch original HTML")
                failed += 1
                continue
            
            # Re-sanitize with improved function
            new_html = clean_html(original_html)
            email.html = new_html
            
            updated += 1
            print("  Updated")
        
        db.commit()
        print(f"\n{'='*50}")
        print(f"Reprocessing complete!")
        print(f"  Updated: {updated}")
        print(f"  Failed:  {failed}")
        
    finally:
        db.close()


if __name__ == "__main__":
    main()
