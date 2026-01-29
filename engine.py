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


# Campaign type keywords for classification
CAMPAIGN_TYPE_KEYWORDS = {
    "Sale": [
        "sale", "discount", "% off", "percent off", "offer", "deal", "save", 
        "clearance", "flash sale", "limited time", "price drop", "markdown",
        "bogo", "buy one get", "extra off", "special offer", "promo"
    ],
    "Welcome": [
        "welcome", "thanks for signing", "thanks for joining", "thank you for subscribing",
        "glad you're here", "nice to meet", "get started", "first order",
        "new member", "welcome aboard", "joined"
    ],
    "Abandoned Cart": [
        "forgot something", "left behind", "still in your cart", "waiting for you",
        "complete your order", "finish your purchase", "cart reminder", "items waiting",
        "don't miss out on", "still interested", "your cart"
    ],
    "Newsletter": [
        "newsletter", "weekly update", "monthly update", "digest", "roundup",
        "this week", "this month", "trending", "what's new", "news from",
        "latest from", "highlights"
    ],
    "New Arrival": [
        "new arrival", "just landed", "just dropped", "new collection", "new launch",
        "introducing", "meet the", "fresh", "just in", "new season",
        "launching", "debut"
    ],
    "Re-engagement": [
        "miss you", "we miss you", "come back", "haven't seen you", "it's been a while",
        "where have you been", "still there", "reconnect", "checking in"
    ],
    "Order Update": [
        "order confirmed", "shipped", "out for delivery", "delivered", "tracking",
        "order status", "shipment", "dispatch", "on its way", "delivery update"
    ],
    "Festive": [
        "diwali", "holi", "christmas", "new year", "eid", "rakhi", "pongal",
        "onam", "navratri", "durga puja", "festival", "festive", "celebration"
    ],
    "Loyalty": [
        "points", "rewards", "loyalty", "member exclusive", "vip", "tier",
        "cashback", "earn", "redeem", "exclusive access"
    ],
    "Feedback": [
        "review", "feedback", "rate us", "how was", "tell us", "survey",
        "share your experience", "your opinion", "rate your"
    ],
}


def extract_campaign_type(subject=None, preview=None, html=None):
    """
    Detect the campaign type based on email content.
    Uses subject line primarily, then preview and HTML content.
    """
    # Combine text for analysis
    text_parts = []
    
    # Subject gets highest priority
    if subject:
        text_parts.append(("subject", subject.lower()))
    if preview:
        text_parts.append(("preview", preview.lower()[:500]))
    if html:
        try:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()
            text_parts.append(("html", soup.get_text(separator=" ").lower()[:2000]))
        except Exception:
            pass
    
    if not text_parts:
        return None
    
    # Score each campaign type
    type_scores = {}
    for campaign_type, keywords in CAMPAIGN_TYPE_KEYWORDS.items():
        score = 0
        for source, text in text_parts:
            for keyword in keywords:
                if keyword in text:
                    # Weight by source (subject > preview > html)
                    if source == "subject":
                        score += 5
                    elif source == "preview":
                        score += 2
                    else:
                        score += 1
        if score > 0:
            type_scores[campaign_type] = score
    
    # Return type with highest score (minimum threshold of 3)
    if type_scores:
        best_type = max(type_scores, key=type_scores.get)
        if type_scores[best_type] >= 3:
            return best_type
    
    return None


def clean_brand_name(name):
    """
    Clean up a brand name by removing common suffixes/prefixes and formatting properly.
    """
    if not name:
        return None
    
    # Common patterns to remove
    patterns_to_remove = [
        # Email-related
        r'\b(newsletter|mailer|noreply|no-reply|donotreply|mail|email|emails)\b',
        r'\b(support|help|info|contact|team|official|india|global)\b',
        r'\b(notifications?|updates?|alerts?|digest)\b',
        # Generic business terms
        r'\b(pvt\.?\s*ltd\.?|private\s*limited|limited|llp|inc\.?|corp\.?)\b',
        r'\b(customer\s*service|customer\s*care)\b',
        # Greetings/common phrases
        r'^(hi|hello|dear|from|the)\s+',
        r'\s+(team|crew|family|club)$',
    ]
    
    cleaned = name.strip()
    for pattern in patterns_to_remove:
        cleaned = re.sub(pattern, '', cleaned, flags=re.IGNORECASE)
    
    # Remove extra whitespace and special characters at edges
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    cleaned = re.sub(r'^[^a-zA-Z0-9]+|[^a-zA-Z0-9]+$', '', cleaned)
    
    # If nothing left or too short, return None
    if not cleaned or len(cleaned) < 2:
        return None
    
    # Proper title case
    return cleaned.title()


def extract_brand(sender, html=None, subject=None):
    """
    Extract brand name from email using multiple smart methods:
    1. Known brand mapping (most reliable)
    2. Email sender display name
    3. Email domain
    4. HTML meta tags (og:site_name, twitter:site)
    5. Logo alt text
    6. Footer copyright text
    7. Subject line brand detection
    """
    from bs4 import BeautifulSoup
    
    candidates = []  # List of (brand_name, confidence_score)
    
    # ===== Method 1: Check known brand mapping first =====
    if sender:
        sender_lower = sender.lower()
        # Check if any known brand is in sender
        for domain, brand_name in BRAND_MAPPING.items():
            if domain in sender_lower:
                return brand_name  # High confidence, return immediately
    
    # ===== Method 2: Extract from sender display name =====
    if sender:
        display_name_match = re.search(r'^([^<]+)<', sender)
        if display_name_match:
            display_name = display_name_match.group(1).strip().strip('"').strip("'")
            cleaned = clean_brand_name(display_name)
            if cleaned and len(cleaned) >= 2:
                candidates.append((cleaned, 80))
    
    # ===== Method 3: Extract from email domain =====
    if sender:
        # Match domain from email like "name@brand.com" or "name@subdomain.brand.co.in"
        domain_match = re.search(r'@([a-zA-Z0-9\-]+(?:\.[a-zA-Z0-9\-]+)*)\.[a-zA-Z]{2,}(?:\.[a-zA-Z]{2,})?', sender)
        if domain_match:
            full_domain = domain_match.group(1)
            domain_parts = full_domain.split('.')
            
            # Get the main brand part (usually first non-generic subdomain)
            generic_subdomains = ['mail', 'email', 'smtp', 'newsletter', 'news', 'marketing', 'promo', 'mg', 'em', 'send']
            brand_part = None
            for part in domain_parts:
                if part.lower() not in generic_subdomains and len(part) > 2:
                    brand_part = part
                    break
            
            if brand_part:
                # Check brand mapping
                if brand_part.lower() in BRAND_MAPPING:
                    return BRAND_MAPPING[brand_part.lower()]
                candidates.append((brand_part.title(), 60))
    
    # ===== Methods 4-6: Extract from HTML =====
    if html:
        try:
            soup = BeautifulSoup(html, "html.parser")
            
            # Method 4a: og:site_name meta tag (very reliable)
            og_site = soup.find('meta', {'property': 'og:site_name'})
            if og_site and og_site.get('content'):
                cleaned = clean_brand_name(og_site.get('content'))
                if cleaned:
                    candidates.append((cleaned, 90))
            
            # Method 4b: twitter:site meta tag
            twitter_site = soup.find('meta', {'name': 'twitter:site'})
            if twitter_site and twitter_site.get('content'):
                site = twitter_site.get('content').lstrip('@')
                cleaned = clean_brand_name(site)
                if cleaned:
                    candidates.append((cleaned, 85))
            
            # Method 4c: application-name meta tag
            app_name = soup.find('meta', {'name': 'application-name'})
            if app_name and app_name.get('content'):
                cleaned = clean_brand_name(app_name.get('content'))
                if cleaned:
                    candidates.append((cleaned, 85))
            
            # Method 5: Logo image alt text
            logo_patterns = ['logo', 'brand', 'header-logo', 'main-logo', 'site-logo']
            for pattern in logo_patterns:
                # Check by class
                logo_img = soup.find('img', class_=re.compile(pattern, re.I))
                if not logo_img:
                    # Check by id
                    logo_img = soup.find('img', id=re.compile(pattern, re.I))
                if not logo_img:
                    # Check by src containing 'logo'
                    logo_img = soup.find('img', src=re.compile(r'logo', re.I))
                
                if logo_img:
                    alt_text = logo_img.get('alt', '')
                    if alt_text and len(alt_text) > 1 and len(alt_text) < 50:
                        cleaned = clean_brand_name(alt_text)
                        if cleaned:
                            candidates.append((cleaned, 75))
                            break
            
            # Method 6: Footer copyright text
            footer = soup.find('footer') or soup.find(class_=re.compile(r'footer', re.I))
            if footer:
                footer_text = footer.get_text()
                # Look for © patterns like "© 2024 BrandName" or "Copyright BrandName"
                copyright_patterns = [
                    r'©\s*\d{4}\s+([A-Za-z][A-Za-z0-9\s&]+?)(?:\s*[,\.|]|$)',
                    r'copyright\s*(?:©?\s*\d{4})?\s+([A-Za-z][A-Za-z0-9\s&]+?)(?:\s*[,\.|]|$)',
                    r'([A-Za-z][A-Za-z0-9\s&]+?)\s*©\s*\d{4}',
                ]
                for pattern in copyright_patterns:
                    match = re.search(pattern, footer_text, re.IGNORECASE)
                    if match:
                        cleaned = clean_brand_name(match.group(1))
                        if cleaned and len(cleaned) >= 2:
                            candidates.append((cleaned, 70))
                            break
            
            # Method 6b: Look for copyright anywhere in email
            if not footer:
                full_text = soup.get_text()
                copyright_match = re.search(r'©\s*\d{4}\s+([A-Za-z][A-Za-z0-9\s&]{2,30}?)(?:\s*[,\.|All]|$)', full_text, re.IGNORECASE)
                if copyright_match:
                    cleaned = clean_brand_name(copyright_match.group(1))
                    if cleaned:
                        candidates.append((cleaned, 65))
            
        except Exception:
            pass
    
    # ===== Method 7: Subject line brand detection =====
    if subject:
        # Look for common patterns like "BrandName: Subject" or "[BrandName] Subject"
        subject_patterns = [
            r'^\[([A-Za-z][A-Za-z0-9\s&]+?)\]',  # [BrandName] Subject
            r'^([A-Za-z][A-Za-z0-9\s&]+?):\s',   # BrandName: Subject
            r'^([A-Za-z][A-Za-z0-9\s&]+?)\s*[-|]\s',  # BrandName - Subject or BrandName | Subject
        ]
        for pattern in subject_patterns:
            match = re.match(pattern, subject)
            if match:
                cleaned = clean_brand_name(match.group(1))
                if cleaned and len(cleaned) >= 2 and len(cleaned) <= 30:
                    candidates.append((cleaned, 50))
                    break
    
    # ===== Select best candidate =====
    if candidates:
        # Sort by confidence score (descending)
        candidates.sort(key=lambda x: x[1], reverse=True)
        
        # Check top candidates against brand mapping
        for brand, score in candidates:
            brand_lower = brand.lower()
            for domain, mapped_name in BRAND_MAPPING.items():
                if domain in brand_lower or brand_lower in domain:
                    return mapped_name
        
        # Return highest confidence candidate
        return candidates[0][0]
    
    return "Unknown"


def safe_filename(text):
    text = text.lower()
    text = re.sub(r'[^a-z0-9]+', '-', text)
    return text[:60].strip('-')


def clean_html(html):
    """
    Minimal HTML cleaning - only remove dangerous executable elements.
    Preserves ALL styling, attributes, and layout for accurate email rendering.
    The iframe sandbox on frontend provides security.
    """
    soup = BeautifulSoup(html, "html.parser")

    # Only remove truly dangerous executable elements
    for tag in soup(["script", "object", "embed"]):
        tag.decompose()
    
    # Remove event handlers from all elements (onclick, onload, etc.)
    for tag in soup.find_all(True):
        attrs_to_remove = [attr for attr in tag.attrs if attr.startswith('on')]
        for attr in attrs_to_remove:
            del tag[attr]

    # Remove tracking pixels (1x1 images) - optional, keeps emails cleaner
    for img in soup.find_all("img"):
        if not isinstance(img, Tag):
            continue
        try:
            width = img.get("width")
            height = img.get("height")
            if width == "1" or height == "1":
                img.decompose()
        except Exception:
            continue

    return str(soup)


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



def fetch_label_emails(label_name: str = LABEL_NAME, max_results: int = 20, fetch_all: bool = False):
    """
    Fetch latest emails for a given Gmail label and return
    a list of structured email records suitable for DB insertion.

    Args:
        label_name: Gmail label to fetch from
        max_results: Max emails per page (up to 500)
        fetch_all: If True, paginate through ALL emails in the label

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

    # Collect all message IDs (with pagination if fetch_all=True)
    all_messages = []
    page_token = None
    page_count = 0
    
    while True:
        page_count += 1
        results = service.users().messages().list(
            userId="me",
            labelIds=[label_id],
            maxResults=min(max_results, 500),  # Gmail API max is 500 per page
            pageToken=page_token
        ).execute()
        
        messages = results.get("messages", [])
        all_messages.extend(messages)
        print(f">>> Page {page_count}: fetched {len(messages)} messages (total so far: {len(all_messages)})")
        
        # Check if we should continue pagination
        page_token = results.get("nextPageToken")
        if not fetch_all or not page_token:
            break
    
    print(f">>> Total messages to process: {len(all_messages)}")

    records = []

    for idx, msg in enumerate(all_messages):
        msg_id = msg["id"]
        if msg_id in processed:
            continue

        # Progress indicator
        if (idx + 1) % 50 == 0:
            print(f">>> Processing email {idx + 1}/{len(all_messages)}...")

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
        # Extract brand with HTML and subject context for better accuracy
        brand = extract_brand(sender, html=cleaned_html, subject=subject)

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
