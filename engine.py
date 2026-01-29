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

# Industry categories
INDUSTRIES = [
    "Men's Fashion",
    "Women's Fashion",
    "Beauty & Personal Care",
    "Food & Beverages",
    "Travel & Hospitality",
    "Electronics & Gadgets",
    "Home & Living",
    "Health & Wellness",
    "Finance & Fintech",
    "Kids & Baby",
    "Sports & Fitness",
    "Entertainment",
    "General Retail",  # Fallback for multi-category retailers
]

# Brand to Industry mapping
BRAND_INDUSTRY_MAPPING = {
    # Beauty & Personal Care
    "nykaa": "Beauty & Personal Care",
    "purplle": "Beauty & Personal Care",
    "mamaearth": "Beauty & Personal Care",
    "sugar": "Beauty & Personal Care",
    "plum": "Beauty & Personal Care",
    "wow": "Beauty & Personal Care",
    "myglamm": "Beauty & Personal Care",
    "minimalist": "Beauty & Personal Care",
    "dot & key": "Beauty & Personal Care",
    "beardo": "Beauty & Personal Care",
    "man matters": "Beauty & Personal Care",
    "mcaffeine": "Beauty & Personal Care",
    "re'equil": "Beauty & Personal Care",
    "forest essentials": "Beauty & Personal Care",
    "kama ayurveda": "Beauty & Personal Care",
    "lakme": "Beauty & Personal Care",
    "colorbar": "Beauty & Personal Care",
    
    # Women's Fashion
    "myntra": "Women's Fashion",
    "ajio": "Women's Fashion",
    "westside": "Women's Fashion",
    "w": "Women's Fashion",
    "biba": "Women's Fashion",
    "fabindia": "Women's Fashion",
    "global desi": "Women's Fashion",
    "zivame": "Women's Fashion",
    "clovia": "Women's Fashion",
    "shein": "Women's Fashion",
    "urbanic": "Women's Fashion",
    "stalkbuylove": "Women's Fashion",
    "faballey": "Women's Fashion",
    "libas": "Women's Fashion",
    
    # Men's Fashion
    "bewakoof": "Men's Fashion",
    "the souled store": "Men's Fashion",
    "snitch": "Men's Fashion",
    "rare rabbit": "Men's Fashion",
    "jack & jones": "Men's Fashion",
    "levis": "Men's Fashion",
    "peter england": "Men's Fashion",
    "van heusen": "Men's Fashion",
    "louis philippe": "Men's Fashion",
    "allen solly": "Men's Fashion",
    
    # Food & Beverages
    "zomato": "Food & Beverages",
    "swiggy": "Food & Beverages",
    "bigbasket": "Food & Beverages",
    "grofers": "Food & Beverages",
    "blinkit": "Food & Beverages",
    "zepto": "Food & Beverages",
    "instamart": "Food & Beverages",
    "dunzo": "Food & Beverages",
    "dominos": "Food & Beverages",
    "mcdonalds": "Food & Beverages",
    "burger king": "Food & Beverages",
    "kfc": "Food & Beverages",
    "pizza hut": "Food & Beverages",
    "starbucks": "Food & Beverages",
    "chaayos": "Food & Beverages",
    "blue tokai": "Food & Beverages",
    "sleepy owl": "Food & Beverages",
    "licious": "Food & Beverages",
    "freshmeat": "Food & Beverages",
    "country delight": "Food & Beverages",
    
    # Travel & Hospitality
    "makemytrip": "Travel & Hospitality",
    "goibibo": "Travel & Hospitality",
    "cleartrip": "Travel & Hospitality",
    "yatra": "Travel & Hospitality",
    "ixigo": "Travel & Hospitality",
    "booking": "Travel & Hospitality",
    "airbnb": "Travel & Hospitality",
    "oyo": "Travel & Hospitality",
    "treebo": "Travel & Hospitality",
    "fabhotels": "Travel & Hospitality",
    "redbus": "Travel & Hospitality",
    "indigo": "Travel & Hospitality",
    "spicejet": "Travel & Hospitality",
    "airindia": "Travel & Hospitality",
    "vistara": "Travel & Hospitality",
    
    # Electronics & Gadgets
    "croma": "Electronics & Gadgets",
    "reliance digital": "Electronics & Gadgets",
    "vijay sales": "Electronics & Gadgets",
    "apple": "Electronics & Gadgets",
    "samsung": "Electronics & Gadgets",
    "oneplus": "Electronics & Gadgets",
    "xiaomi": "Electronics & Gadgets",
    "realme": "Electronics & Gadgets",
    "boat": "Electronics & Gadgets",
    "noise": "Electronics & Gadgets",
    "fire-boltt": "Electronics & Gadgets",
    "pebble": "Electronics & Gadgets",
    
    # Home & Living
    "pepperfry": "Home & Living",
    "urban ladder": "Home & Living",
    "hometown": "Home & Living",
    "ikea": "Home & Living",
    "home centre": "Home & Living",
    "nestasia": "Home & Living",
    "ellementry": "Home & Living",
    "wooden street": "Home & Living",
    "sleepycat": "Home & Living",
    "wakefit": "Home & Living",
    "sunday": "Home & Living",
    
    # Health & Wellness
    "pharmeasy": "Health & Wellness",
    "netmeds": "Health & Wellness",
    "1mg": "Health & Wellness",
    "apollo pharmacy": "Health & Wellness",
    "healthkart": "Health & Wellness",
    "cult.fit": "Health & Wellness",
    "cure.fit": "Health & Wellness",
    "practo": "Health & Wellness",
    "mfine": "Health & Wellness",
    "truweight": "Health & Wellness",
    "oziva": "Health & Wellness",
    "kapiva": "Health & Wellness",
    
    # Finance & Fintech
    "paytm": "Finance & Fintech",
    "phonepe": "Finance & Fintech",
    "gpay": "Finance & Fintech",
    "google pay": "Finance & Fintech",
    "cred": "Finance & Fintech",
    "slice": "Finance & Fintech",
    "jupiter": "Finance & Fintech",
    "fi": "Finance & Fintech",
    "groww": "Finance & Fintech",
    "zerodha": "Finance & Fintech",
    "upstox": "Finance & Fintech",
    "policybazaar": "Finance & Fintech",
    "acko": "Finance & Fintech",
    "digit": "Finance & Fintech",
    
    # Kids & Baby
    "firstcry": "Kids & Baby",
    "hopscotch": "Kids & Baby",
    "babyhug": "Kids & Baby",
    "the mom co": "Kids & Baby",
    "mamaearth baby": "Kids & Baby",
    "mothercare": "Kids & Baby",
    "the moms co": "Kids & Baby",
    
    # Sports & Fitness
    "decathlon": "Sports & Fitness",
    "puma": "Sports & Fitness",
    "nike": "Sports & Fitness",
    "adidas": "Sports & Fitness",
    "reebok": "Sports & Fitness",
    "asics": "Sports & Fitness",
    "skechers": "Sports & Fitness",
    "hrx": "Sports & Fitness",
    
    # Entertainment
    "bookmyshow": "Entertainment",
    "netflix": "Entertainment",
    "amazon prime": "Entertainment",
    "hotstar": "Entertainment",
    "disney": "Entertainment",
    "zee5": "Entertainment",
    "sony liv": "Entertainment",
    "jio cinema": "Entertainment",
    "spotify": "Entertainment",
    "gaana": "Entertainment",
    "wynk": "Entertainment",
    
    # General Retail (multi-category)
    "flipkart": "General Retail",
    "amazon": "General Retail",
    "snapdeal": "General Retail",
    "meesho": "General Retail",
    "tatacliq": "General Retail",
    "reliance": "General Retail",
    "jiomart": "General Retail",
}


# Industry keywords for content-based classification
INDUSTRY_KEYWORDS = {
    "Beauty & Personal Care": [
        "skincare", "makeup", "cosmetic", "lipstick", "mascara", "foundation", 
        "serum", "moisturizer", "sunscreen", "face wash", "cleanser", "toner",
        "beauty", "glow", "skin", "haircare", "shampoo", "conditioner", "hair oil",
        "perfume", "fragrance", "deodorant", "grooming", "salon", "spa",
        "nail polish", "eye shadow", "blush", "concealer", "primer",
    ],
    "Women's Fashion": [
        "saree", "kurti", "lehenga", "salwar", "dupatta", "ethnic wear",
        "women's", "ladies", "dress", "gown", "skirt", "blouse", "top",
        "lingerie", "bra", "panties", "nightwear", "western wear",
        "handbag", "clutch", "earrings", "necklace", "jewelry", "jewellery",
        "heels", "sandals", "flats", "women footwear",
    ],
    "Men's Fashion": [
        "men's", "shirt", "trouser", "jeans", "t-shirt", "polo", "blazer",
        "suit", "formal wear", "casual wear", "men footwear", "sneakers",
        "wallet", "belt", "tie", "cufflinks", "watch", "sunglasses",
        "kurta", "sherwani", "ethnic men",
    ],
    "Food & Beverages": [
        "food", "restaurant", "order", "delivery", "hungry", "eat", "meal",
        "pizza", "burger", "biryani", "curry", "cuisine", "chef",
        "grocery", "vegetables", "fruits", "fresh", "organic",
        "coffee", "tea", "juice", "smoothie", "dessert", "cake", "sweet",
        "meat", "chicken", "fish", "seafood", "mutton",
        "snacks", "breakfast", "lunch", "dinner", "menu",
    ],
    "Travel & Hospitality": [
        "flight", "hotel", "booking", "travel", "trip", "vacation", "holiday",
        "destination", "airport", "airline", "boarding", "check-in",
        "resort", "stay", "accommodation", "room", "suite",
        "bus", "train", "cab", "taxi", "car rental",
        "tour", "package", "itinerary", "passport", "visa",
    ],
    "Electronics & Gadgets": [
        "phone", "mobile", "smartphone", "laptop", "computer", "tablet",
        "earbuds", "headphones", "speaker", "smartwatch", "wearable",
        "television", "tv", "camera", "appliance", "gadget", "tech",
        "charger", "cable", "accessory", "electronic",
        "gaming", "console", "processor", "ram", "storage",
    ],
    "Home & Living": [
        "furniture", "sofa", "bed", "mattress", "pillow", "table", "chair",
        "decor", "home", "living room", "bedroom", "kitchen", "bathroom",
        "curtain", "carpet", "rug", "lamp", "lighting",
        "storage", "organizer", "shelf", "wardrobe", "cupboard",
        "kitchenware", "cookware", "utensil", "dinnerware",
    ],
    "Health & Wellness": [
        "medicine", "pharmacy", "health", "wellness", "vitamin", "supplement",
        "doctor", "consultation", "prescription", "tablet", "capsule",
        "fitness", "gym", "workout", "exercise", "yoga", "meditation",
        "protein", "nutrition", "diet", "weight loss", "immunity",
        "ayurveda", "herbal", "natural remedy",
    ],
    "Finance & Fintech": [
        "payment", "transaction", "transfer", "upi", "wallet", "money",
        "credit card", "debit card", "emi", "loan", "insurance",
        "invest", "mutual fund", "stock", "trading", "portfolio",
        "bank", "account", "savings", "fd", "deposit",
        "bill", "recharge", "cashback", "reward", "offer",
    ],
    "Kids & Baby": [
        "baby", "kids", "child", "toddler", "infant", "newborn",
        "diaper", "feeding", "stroller", "crib", "toy",
        "kids wear", "baby clothes", "school", "nursery",
        "parenting", "mom", "mother", "pregnancy", "maternity",
    ],
    "Sports & Fitness": [
        "sports", "athletic", "running", "jogging", "cycling", "swimming",
        "football", "cricket", "badminton", "tennis", "gym wear",
        "sportswear", "activewear", "tracksuit", "sneakers", "sports shoes",
        "fitness tracker", "equipment", "outdoor", "adventure",
    ],
    "Entertainment": [
        "movie", "film", "cinema", "theatre", "show", "concert", "event",
        "streaming", "watch", "series", "episode", "season",
        "music", "song", "playlist", "podcast", "audio",
        "game", "gaming", "play", "ticket", "booking",
    ],
}


def extract_industry(brand_name, subject=None, preview=None, html=None):
    """
    Extract industry using multiple methods:
    1. Brand name mapping (most accurate)
    2. Content-based keyword analysis (fallback)
    """
    # Method 1: Brand name mapping
    if brand_name:
        brand_lower = brand_name.lower().strip()
        
        # Direct lookup
        if brand_lower in BRAND_INDUSTRY_MAPPING:
            return BRAND_INDUSTRY_MAPPING[brand_lower]
        
        # Partial match
        for key, industry in BRAND_INDUSTRY_MAPPING.items():
            if key in brand_lower or brand_lower in key:
                return industry
    
    # Method 2: Content-based keyword analysis
    # Combine all available text
    text_parts = []
    if subject:
        text_parts.append(subject.lower())
    if preview:
        text_parts.append(preview.lower())
    if html:
        # Extract text from HTML (simple approach - just get visible text)
        from bs4 import BeautifulSoup
        try:
            soup = BeautifulSoup(html, "html.parser")
            # Remove script and style elements
            for script in soup(["script", "style"]):
                script.decompose()
            text_parts.append(soup.get_text(separator=" ").lower()[:5000])  # Limit to first 5000 chars
        except Exception:
            pass
    
    if not text_parts:
        return None
    
    combined_text = " ".join(text_parts)
    
    # Score each industry based on keyword matches
    industry_scores = {}
    for industry, keywords in INDUSTRY_KEYWORDS.items():
        score = 0
        for keyword in keywords:
            if keyword in combined_text:
                # Weight by keyword position (earlier = more important)
                score += 1
                # Bonus if keyword is in subject
                if subject and keyword in subject.lower():
                    score += 2
        if score > 0:
            industry_scores[industry] = score
    
    # Return industry with highest score (if any matches found)
    if industry_scores:
        return max(industry_scores, key=industry_scores.get)
    
    return None


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
    """
    Clean HTML while preserving email formatting.
    Only removes dangerous elements (script, iframe, form) and tracking pixels.
    Preserves all styling and layout elements for accurate email rendering.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Remove dangerous/executable elements
    for tag in soup(["script", "iframe", "form", "object", "embed"]):
        tag.decompose()

    # Remove tracking pixels (1x1 images)
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

    # Email-safe HTML tags - comprehensive list for proper email rendering
    ALLOWED_TAGS = [
        # Text formatting
        "p", "br", "hr", "h1", "h2", "h3", "h4", "h5", "h6",
        "b", "i", "u", "s", "strong", "em", "small", "mark", "del", "ins", "sub", "sup",
        "span", "div", "font", "center",
        # Lists
        "ul", "ol", "li", "dl", "dt", "dd",
        # Tables (critical for email layouts)
        "table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption", "colgroup", "col",
        # Links and media
        "a", "img",
        # Semantic/structural
        "article", "section", "header", "footer", "nav", "aside", "main",
        "blockquote", "pre", "code", "address",
        # Style
        "style",
    ]

    # Attributes commonly used in email HTML
    ALLOWED_ATTRIBUTES = {
        "*": ["style", "class", "id", "dir", "lang", "title"],
        "a": ["href", "title", "target", "rel"],
        "img": ["src", "alt", "width", "height", "border"],
        "table": ["width", "height", "cellpadding", "cellspacing", "border", "bgcolor", "align", "valign", "role"],
        "tr": ["bgcolor", "align", "valign", "height"],
        "td": ["width", "height", "bgcolor", "align", "valign", "colspan", "rowspan", "nowrap"],
        "th": ["width", "height", "bgcolor", "align", "valign", "colspan", "rowspan", "scope"],
        "font": ["color", "face", "size"],
        "div": ["align"],
        "p": ["align"],
        "span": ["align"],
        "col": ["width", "span"],
        "colgroup": ["span"],
    }

    return bleach.clean(
        str(soup),
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRIBUTES,
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
