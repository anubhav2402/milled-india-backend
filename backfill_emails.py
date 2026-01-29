"""
Backfill script to fetch older emails from Gmail using pagination.
This will fetch emails in batches going back in time until it hits emails already in the DB.

Usage:
    python backfill_emails.py [--max-batches 10] [--batch-size 100]
    
Example: Fetch up to 10 batches of 100 emails each (1000 emails total)
    python backfill_emails.py --max-batches 10 --batch-size 100
"""

import argparse
import os
from datetime import datetime
from email.utils import parsedate_to_datetime

from sqlalchemy.orm import Session

from backend.db import SessionLocal, engine, Base
from backend.models import Email
from engine import authenticate, get_label_id, extract_body, clean_html, extract_brand
from googleapiclient.discovery import build
from bs4 import BeautifulSoup


def fetch_emails_batch(service, label_id, max_results=100, page_token=None):
    """Fetch a batch of emails with optional pagination."""
    params = {
        "userId": "me",
        "labelIds": [label_id],
        "maxResults": max_results,
    }
    if page_token:
        params["pageToken"] = page_token
    
    return service.users().messages().list(**params).execute()


def process_email(service, msg_id, db: Session):
    """Process a single email and insert if not exists."""
    existing = db.query(Email).filter(Email.gmail_id == msg_id).first()
    if existing:
        return False  # Already exists
    
    message = service.users().messages().get(
        userId="me",
        id=msg_id,
        format="full"
    ).execute()
    
    headers = {h["name"]: h["value"] for h in message["payload"]["headers"]}
    subject = headers.get("Subject", "no-subject")
    sender = headers.get("From", "")
    received_ts = headers.get("Date")
    
    html = extract_body(message["payload"])
    if not html:
        return False
    
    cleaned_html = clean_html(html)
    # Extract brand with HTML context for better accuracy
    brand = extract_brand(sender, cleaned_html)
    
    soup = BeautifulSoup(cleaned_html, "html.parser")
    preview_text = soup.get_text(separator=" ", strip=True)[:200]
    
    raw_received = received_ts or datetime.now().isoformat()
    if isinstance(raw_received, str):
        try:
            received_dt = parsedate_to_datetime(raw_received)
        except Exception:
            received_dt = datetime.utcnow()
    else:
        received_dt = raw_received
    
    email = Email(
        gmail_id=msg_id,
        subject=subject,
        sender=sender,
        brand=brand,
        category=None,
        type=None,
        received_at=received_dt,
        html=cleaned_html,
        preview=preview_text,
    )
    db.add(email)
    return True


def main():
    parser = argparse.ArgumentParser(description="Backfill older emails from Gmail")
    parser.add_argument("--max-batches", type=int, default=10, help="Maximum number of batches to fetch")
    parser.add_argument("--batch-size", type=int, default=100, help="Emails per batch")
    args = parser.parse_args()
    
    Base.metadata.create_all(bind=engine)
    
    creds = authenticate()
    service = build("gmail", "v1", credentials=creds)
    label_id = get_label_id(service, label_name="Search Engine")
    
    db = SessionLocal()
    total_created = 0
    total_skipped = 0
    batch_num = 0
    page_token = None
    
    try:
        while batch_num < args.max_batches:
            batch_num += 1
            print(f"\n>>> Fetching batch {batch_num} (max {args.max_batches})...")
            
            results = fetch_emails_batch(service, label_id, args.batch_size, page_token)
            messages = results.get("messages", [])
            
            if not messages:
                print("No more messages found.")
                break
            
            print(f">>> Processing {len(messages)} emails in this batch...")
            
            batch_created = 0
            batch_skipped = 0
            
            for msg in messages:
                msg_id = msg["id"]
                if process_email(service, msg_id, db):
                    batch_created += 1
                else:
                    batch_skipped += 1
            
            db.commit()
            total_created += batch_created
            total_skipped += batch_skipped
            
            print(f">>> Batch {batch_num}: Created {batch_created}, Skipped {batch_skipped}")
            
            # Get next page token
            page_token = results.get("nextPageToken")
            if not page_token:
                print("No more pages available.")
                break
            
    finally:
        db.close()
    
    print(f"\n=== Backfill complete ===")
    print(f"Total batches: {batch_num}")
    print(f"Total created: {total_created}")
    print(f"Total skipped (existing): {total_skipped}")


if __name__ == "__main__":
    main()
