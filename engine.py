import os
import re
import pickle
import base64
from datetime import datetime

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build

from bs4 import BeautifulSoup
from bs4.element import Tag

import bleach

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']
LABEL_NAME = 'Search Engine'
PROCESSED_FILE = 'processed_ids.txt'
OUTPUT_DIR = 'emails'

def _use_processed_file() -> bool:
    """
    On servers (Render cron), local filesystems may not be persistent.
    Default to NOT using processed_ids.txt and rely on DB de-duplication instead.
    """
    return os.getenv("USE_PROCESSED_FILE", "").strip().lower() in {"1", "true", "yes", "y"}


def authenticate():
    # Server-friendly auth (Render cron/job): use env vars instead of local files/browser OAuth.
    client_id = os.getenv("GOOGLE_CLIENT_ID")
    client_secret = os.getenv("GOOGLE_CLIENT_SECRET")
    refresh_token = os.getenv("GOOGLE_REFRESH_TOKEN")
    if client_id and client_secret and refresh_token:
        creds = Credentials(
            token=None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=SCOPES,
        )
        # Force refresh to obtain an access token
        creds.refresh(Request())
        return creds

    # Fallback to local file-based auth (for local development)
    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    return creds


def load_processed_ids():
    if not _use_processed_file():
        return set()
    if not os.path.exists(PROCESSED_FILE):
        return set()
    with open(PROCESSED_FILE, 'r') as f:
        return set(line.strip() for line in f)


def save_processed_id(msg_id):
    if not _use_processed_file():
        return
    with open(PROCESSED_FILE, 'a') as f:
        f.write(msg_id + '\n')


def get_label_id(service, label_name: str = LABEL_NAME):
    labels = service.users().labels().list(userId='me').execute()
    for label in labels['labels']:
        if label['name'] == label_name:
            return label['id']
    raise Exception(f"Label {label_name} not found")


# Brand mapping for known Indian D2C brands
BRAND_MAPPING = {
    # Domain -> Brand Name mappings
    "nykaa": "Nykaa",
    "myntra": "Myntra",
    "zomato": "Zomato",
    "swiggy": "Swiggy",
    "meesho": "Meesho",
    "mamaearth": "Mamaearth",
    "purplle": "Purplle",
    "firstcry": "FirstCry",
    "tatacliq": "Tata CLiQ",
    "ajio": "AJIO",
    "flipkart": "Flipkart",
    "amazon": "Amazon",
    "snapdeal": "Snapdeal",
    "paytm": "Paytm",
    "bigbasket": "BigBasket",
    "grofers": "Grofers",
    "croma": "Croma",
    "reliance": "Reliance",
    "reliance digital": "Reliance Digital",
    # Add more mappings as needed
}


def extract_brand(sender, html=None):
    """
    Extract brand name from email sender using multiple methods:
    1. Extract display name from "Brand Name <email@domain.com>" format
    2. Check brand mapping dictionary
    3. Extract from domain name
    4. Try to extract from email HTML (if provided)
    """
    if not sender:
        return "unknown"
    
    sender_lower = sender.lower()
    
    # Method 1: Extract display name from "Brand Name <email@domain.com>" format
    # Example: "Nykaa <noreply@nykaa.com>" -> "Nykaa"
    display_name_match = re.search(r'^([^<]+)<', sender)
    if display_name_match:
        display_name = display_name_match.group(1).strip().strip('"').strip("'")
        # Clean up common prefixes/suffixes
        display_name = re.sub(r'^(noreply|no-reply|donotreply|donot-reply|mailer|newsletter)\s*[-:]?\s*', '', display_name, flags=re.IGNORECASE)
        display_name = display_name.strip()
        # If it looks like a brand name (not just an email), use it
        if display_name and len(display_name) > 2 and '@' not in display_name:
            # Check if it matches a known brand
            for domain, brand_name in BRAND_MAPPING.items():
                if domain in display_name.lower():
                    return brand_name
            return display_name.title()  # Capitalize properly
    
    # Method 2: Extract domain and check mapping
    domain_match = re.search(r'@([\w\-]+(?:\.[\w\-]+)*)\.', sender_lower)
    if domain_match:
        domain = domain_match.group(1)
        # Handle subdomains - take the main domain
        domain_parts = domain.split('.')
        main_domain = domain_parts[-1] if len(domain_parts) > 1 else domain
        
        # Check brand mapping
        if main_domain in BRAND_MAPPING:
            return BRAND_MAPPING[main_domain]
        
        # For known patterns, extract brand name
        if main_domain in ['com', 'in', 'co'] and len(domain_parts) > 1:
            brand_part = domain_parts[-2]  # e.g., "nykaa.com" -> "nykaa"
            if brand_part in BRAND_MAPPING:
                return BRAND_MAPPING[brand_part]
            return brand_part.title()
        
        return main_domain.title()
    
    # Method 3: Try to extract from HTML if provided
    if html:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            # Look for common brand indicators in HTML
            # Check title tag
            title = soup.find('title')
            if title:
                title_text = title.get_text().lower()
                for domain, brand_name in BRAND_MAPPING.items():
                    if domain in title_text:
                        return brand_name
            
            # Check meta tags
            meta_brand = soup.find('meta', {'name': re.compile(r'brand|company', re.I)})
            if meta_brand and meta_brand.get('content'):
                content = meta_brand.get('content').lower()
                for domain, brand_name in BRAND_MAPPING.items():
                    if domain in content:
                        return brand_name
            
            # Check for brand in common class names or IDs
            brand_elements = soup.find_all(class_=re.compile(r'brand|logo|company', re.I))
            for elem in brand_elements[:3]:  # Check first 3 matches
                text = elem.get_text().strip()
                if text and len(text) < 50:  # Reasonable brand name length
                    for domain, brand_name in BRAND_MAPPING.items():
                        if domain in text.lower():
                            return brand_name
        except Exception:
            pass  # If HTML parsing fails, continue
    
    return "unknown"


def safe_filename(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text[:60].strip('-')


def clean_html(html):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "iframe"]):
        tag.decompose()

    for img in soup.find_all("img"):
        if not isinstance(img, Tag):
            continue

        try:
            width = img.get("width")
            height = img.get("height")

            if width == "1" or height == "1":
                img.decompose()
        except Exception:
            # Never let broken HTML crash ingestion
            continue

    return bleach.clean(
        str(soup),
        tags=list(bleach.sanitizer.ALLOWED_TAGS) +
        ["img", "table", "tr", "td", "th", "style", "tbody", "thead"],
        attributes={
            "*": ["style"],
            "a": ["href", "title"],
            "img": ["src", "alt", "width", "height"]
        },
        strip=True
    )


def extract_body(payload):
    # Case 1: simple HTML email (VERY COMMON)
    if payload.get("mimeType") == "text/html" and payload.get("body", {}).get("data"):
        return base64.urlsafe_b64decode(
            payload["body"]["data"]
        ).decode("utf-8", errors="ignore")

    html = None

    def walk(parts):
        nonlocal html
        for part in parts:
            mime = part.get("mimeType")
            body = part.get("body", {}).get("data")

            if mime == "text/html" and body:
                html = base64.urlsafe_b64decode(
                    body
                ).decode("utf-8", errors="ignore")
                return

            if part.get("parts"):
                walk(part["parts"])

    if payload.get("parts"):
        walk(payload["parts"])

    return html



def fetch_label_emails(label_name: str = LABEL_NAME, max_results: int = 20):
    """
    Fetch latest emails for a given Gmail label and return
    a list of structured email records suitable for DB insertion.

    Each record has keys:
    - gmail_id
    - subject
    - sender
    - brand
    - received_at (ISO 8601 string)
    - html (cleaned)
    - preview (short text)
    """
    creds = authenticate()
    service = build("gmail", "v1", credentials=creds)

    label_id = get_label_id(service, label_name=label_name)
    processed = load_processed_ids()

    results = service.users().messages().list(
        userId="me",
        labelIds=[label_id],
        maxResults=max_results
    ).execute()

    print(">>> messages fetched:", len(results.get("messages", [])))

    records = []

    for msg in results.get("messages", []):
        msg_id = msg["id"]
        if msg_id in processed:
            continue

        message = service.users().messages().get(
            userId="me",
            id=msg_id,
            format="full"
        ).execute()

        headers = {h["name"]: h["value"]
                   for h in message["payload"]["headers"]}

        subject = headers.get("Subject", "no-subject")
        sender = headers.get("From", "")
        received_ts = headers.get("Date")

        html = extract_body(message["payload"])
        if not html:
            save_processed_id(msg_id)
            continue

        cleaned_html = clean_html(html)
        # Extract brand with HTML context for better accuracy
        brand = extract_brand(sender, cleaned_html)

        # Simple preview: first 200 visible characters
        soup = BeautifulSoup(cleaned_html, "html.parser")
        preview_text = soup.get_text(separator=" ", strip=True)[:200]

        record = {
            "gmail_id": msg_id,
            "subject": subject,
            "sender": sender,
            "brand": brand,
            "received_at": received_ts or datetime.now().isoformat(),
            "html": cleaned_html,
            "preview": preview_text,
        }
        records.append(record)

        save_processed_id(msg_id)

    return records


def main():
    """CLI entrypoint for quick manual testing."""
    print(">>> Fetching latest emails from label:", LABEL_NAME)
    emails = fetch_label_emails()
    print(f"Fetched {len(emails)} new emails.")
    # Optionally still write to disk for debugging
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for e in emails:
        brand = e["brand"]
        date = datetime.now().strftime("%Y-%m-%d")
        filename = safe_filename(e["subject"])
        folder = os.path.join(OUTPUT_DIR, brand)
        os.makedirs(folder, exist_ok=True)
        path = f"{folder}/{date}_{filename}.html"
        with open(path, "w", encoding="utf-8") as f:
            f.write(e["html"])
        print("Saved →", path)


if __name__ == "__main__":
    main()
