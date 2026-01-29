from datetime import datetime
from email.utils import parsedate_to_datetime

from sqlalchemy.orm import Session

from backend.db import SessionLocal, engine, Base
from backend.models import Email
from engine import fetch_label_emails, extract_industry


def upsert_emails(db: Session):
    # Fetch emails - configurable via env vars
    # GMAIL_MAX_RESULTS: emails per page (default 100)
    # GMAIL_FETCH_ALL: if "true", fetch ALL emails from label (use for initial sync)
    # BATCH_SIZE: commit every N emails to avoid connection timeouts (default 50)
    import os
    max_results = int(os.getenv("GMAIL_MAX_RESULTS", "100"))
    fetch_all = os.getenv("GMAIL_FETCH_ALL", "").lower() in ("true", "1", "yes")
    batch_size = int(os.getenv("BATCH_SIZE", "50"))
    
    if fetch_all:
        print(">>> GMAIL_FETCH_ALL is enabled - fetching ALL emails from label...")
    
    records = fetch_label_emails(max_results=max_results, fetch_all=fetch_all)
    created = 0
    skipped = 0
    batch_count = 0

    for idx, r in enumerate(records):
        existing = db.query(Email).filter(Email.gmail_id == r["gmail_id"]).first()
        if existing:
            skipped += 1
            continue

        # Gmail Date header is usually RFC 2822, not ISO 8601
        raw_received = r["received_at"]
        if isinstance(raw_received, str):
            try:
                received_dt = parsedate_to_datetime(raw_received)
            except Exception:
                received_dt = datetime.utcnow()
        else:
            received_dt = raw_received

        # Auto-detect industry from brand and content
        brand = r["brand"]
        industry = extract_industry(
            brand_name=brand,
            subject=r["subject"],
            preview=r["preview"],
            html=r["html"]
        )

        email = Email(
            gmail_id=r["gmail_id"],
            subject=r["subject"],
            sender=r["sender"],
            brand=brand,
            category=None,
            type=None,
            industry=industry,
            received_at=received_dt,
            html=r["html"],
            preview=r["preview"],
        )
        db.add(email)
        created += 1
        batch_count += 1

        # Commit in batches to avoid connection timeouts
        if batch_count >= batch_size:
            db.commit()
            print(f">>> Committed batch ({created} created so far, {skipped} skipped)")
            batch_count = 0

    # Commit any remaining emails
    if batch_count > 0:
        db.commit()
        print(f">>> Final commit ({created} created total)")
    
    return created, skipped


def main():
    # Ensure tables exist before we start using the session
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        created, skipped = upsert_emails(db)
        print(f"Ingestion complete. Created: {created}, Skipped (existing): {skipped}")
    finally:
        db.close()


if __name__ == "__main__":
    main()

