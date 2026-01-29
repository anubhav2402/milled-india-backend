from datetime import datetime
from email.utils import parsedate_to_datetime

from sqlalchemy.orm import Session

from backend.db import SessionLocal, engine, Base
from backend.models import Email
from engine import fetch_label_emails, extract_industry


def upsert_emails(db: Session):
    # Fetch more emails per run (configurable via env var, default 100)
    import os
    max_results = int(os.getenv("GMAIL_MAX_RESULTS", "100"))
    records = fetch_label_emails(max_results=max_results)
    created = 0
    skipped = 0

    for r in records:
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

    db.commit()
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

