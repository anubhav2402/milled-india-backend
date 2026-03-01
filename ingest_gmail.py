import base64
import os
from datetime import datetime
from email.utils import parsedate_to_datetime

from bs4 import BeautifulSoup
from googleapiclient.discovery import build
from sqlalchemy.orm import Session

from backend.db import SessionLocal, engine, Base
from backend.models import Email
from engine import (
    authenticate, get_label_id, extract_body, clean_html,
    extract_brand, extract_industry, extract_campaign_type,
    fetch_label_emails, LABEL_NAME,
)


def fast_ingest(db: Session):
    """
    Page-by-page streaming ingestion.
    Lists 500 message IDs at a time, skips already-known ones,
    fetches + processes only new emails, commits per page.
    """
    # 1. Load all known gmail_ids upfront (one fast query)
    existing_ids = set(row[0] for row in db.query(Email.gmail_id).all())
    print(f">>> {len(existing_ids)} emails already in DB — will skip these")

    # 2. Authenticate and get label
    creds = authenticate()
    service = build("gmail", "v1", credentials=creds)
    label_id = get_label_id(service)

    total_created = 0
    total_skipped = 0
    page_count = 0
    page_token = None

    # 3. Page-by-page: list → filter → fetch → ingest → commit
    while True:
        page_count += 1
        try:
            results = service.users().messages().list(
                userId="me",
                labelIds=[label_id],
                maxResults=500,
                pageToken=page_token,
            ).execute()
        except Exception as e:
            print(f">>> Page {page_count} listing failed: {e}")
            print(f">>> Stopping pagination. {total_created} emails ingested so far.")
            break

        messages = results.get("messages", [])
        if not messages:
            break

        # Filter out already-ingested
        new_ids = [m["id"] for m in messages if m["id"] not in existing_ids]
        skipped = len(messages) - len(new_ids)
        total_skipped += skipped

        print(f">>> Page {page_count}: {len(messages)} listed, {skipped} already in DB, {len(new_ids)} new")

        # Fetch and process new emails
        errors = 0
        for idx, msg_id in enumerate(new_ids):
            try:
                message = service.users().messages().get(
                    userId="me", id=msg_id, format="full"
                ).execute()
            except Exception as e:
                errors += 1
                print(f">>>   Failed to fetch {msg_id}: {e}")
                if errors > 10:
                    print(">>>   Too many fetch errors, skipping rest of page")
                    break
                continue

            headers = {h["name"]: h["value"] for h in message["payload"]["headers"]}
            subject = headers.get("Subject", "no-subject")
            sender = headers.get("From", "")
            received_ts = headers.get("Date")

            html = extract_body(message["payload"])
            if not html:
                continue

            cleaned_html = clean_html(html)
            brand = extract_brand(sender, html=cleaned_html, subject=subject)

            soup = BeautifulSoup(cleaned_html, "html.parser")
            preview_text = soup.get_text(separator=" ", strip=True)[:200]

            # Parse date
            received_dt = datetime.utcnow()
            if received_ts:
                try:
                    received_dt = parsedate_to_datetime(received_ts)
                except Exception:
                    pass

            classification = extract_industry(
                brand_name=brand, subject=subject,
                preview=preview_text, html=cleaned_html, db_session=db,
                return_dict=True,
            )
            industry = classification["industry"] if isinstance(classification, dict) else classification
            category = classification.get("category") if isinstance(classification, dict) else None
            campaign_type = extract_campaign_type(
                subject=subject, preview=preview_text,
                html=cleaned_html, brand_name=brand, use_ai=True,
            )

            email = Email(
                gmail_id=msg_id,
                subject=subject,
                sender=sender,
                brand=brand,
                category=category,
                type=campaign_type,
                industry=industry,
                received_at=received_dt,
                html=cleaned_html,
                preview=preview_text,
            )
            try:
                db.add(email)
                db.flush()  # detect duplicates early
            except Exception:
                db.rollback()
                total_skipped += 1
                continue
            existing_ids.add(msg_id)
            total_created += 1

        # Commit after each page
        try:
            db.commit()
        except Exception:
            db.rollback()
        print(f">>>   Committed. Total so far: {total_created} created, {total_skipped} skipped")

        # Next page
        page_token = results.get("nextPageToken")
        if not page_token:
            break

    print(f"\n>>> Ingestion complete. Created: {total_created}, Skipped: {total_skipped}")
    return total_created, total_skipped


def upsert_emails(db: Session):
    # Fetch emails - configurable via env vars
    # GMAIL_MAX_RESULTS: emails per page (default 100)
    # GMAIL_FETCH_ALL: if "true", fetch ALL emails from label (use for initial sync)
    # BATCH_SIZE: commit every N emails to avoid connection timeouts (default 50)
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

        # Auto-detect industry from brand and content (with DB caching)
        brand = r["brand"]
        classification = extract_industry(
            brand_name=brand,
            subject=r["subject"],
            preview=r["preview"],
            html=r["html"],
            db_session=db,
            return_dict=True,
        )
        industry = classification["industry"] if isinstance(classification, dict) else classification
        category = classification.get("category") if isinstance(classification, dict) else None

        # Auto-detect campaign type (with AI fallback)
        campaign_type = extract_campaign_type(
            subject=r["subject"],
            preview=r["preview"],
            html=r["html"],
            brand_name=brand,
            use_ai=True  # Enable AI classification for uncertain cases
        )

        email = Email(
            gmail_id=r["gmail_id"],
            subject=r["subject"],
            sender=r["sender"],
            brand=brand,
            category=category,
            type=campaign_type,
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
    import sys
    Base.metadata.create_all(bind=engine)

    db = SessionLocal()
    try:
        if "--fast" in sys.argv or os.getenv("GMAIL_FETCH_ALL", "").lower() in ("true", "1", "yes"):
            created, skipped = fast_ingest(db)
        else:
            created, skipped = upsert_emails(db)
        print(f"Ingestion complete. Created: {created}, Skipped (existing): {skipped}")
    finally:
        db.close()


if __name__ == "__main__":
    main()
