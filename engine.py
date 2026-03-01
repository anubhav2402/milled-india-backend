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

# Industry categories (must match ai_classifier.py)
INDUSTRIES = [
    "Apparel & Accessories",
    "Baby & Kids",
    "Beauty & Personal Care",
    "Books, Art & Stationery",
    "Business & B2B Retail",
    "Electronics & Tech",
    "Entertainment",
    "Finance & Fintech",
    "Food & Beverage",
    "General / Department Store",
    "Gifts & Lifestyle",
    "Health, Fitness & Wellness",
    "Home & Living",
    "Luxury & High-End Goods",
    "Pets",
    "Tools, Auto & DIY",
    "Travel & Outdoors",
]

# Suffixes/variants to strip when normalizing brand names for mapping lookup
BRAND_SUFFIXES_TO_STRIP = [
    r'\s*[-–—_,]\s*a\s+tata\s+product',
    r'\s*[-–—_,]\s*a\s+tanishq\s+partnership',
    r'\s*[-–—_,]\s*a\s+godrej\s+\w+\s+brand',
    r'\s*,?\s*a\s+\w+\s+(brand|product|partnership)',
    r'\s+(sale|promotion|promotions|rewards|new arrivals|black friday|cyber monday)$',
    r'\s+at\s+\w+$',       # "Maeve At Brooklinen"
    r'\s+by\s+\w+$',       # "Maeve By Anthropologie"
    r'\+\s*clinic$',        # "Supertails+ Clinic"
]


def normalize_brand_name(brand):
    """
    Strip suffixes/variants to get the core brand name for mapping lookup.
    E.g. "Caratlane - A Tata Product" → "caratlane"
         "Anthropologie Sale" → "anthropologie"
         "Net-A-Porter Rewards" → "net-a-porter"
    """
    if not brand:
        return ""
    normalized = brand.lower().strip()
    for pattern in BRAND_SUFFIXES_TO_STRIP:
        normalized = re.sub(pattern, '', normalized, flags=re.IGNORECASE).strip()
    return normalized


# Brand to Industry mapping - COMPREHENSIVE
# This mapping covers all known brands for accurate classification
BRAND_INDUSTRY_MAPPING = {
    # ============ Beauty & Personal Care ============
    # Indian brands
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
    "foxtale": "Beauty & Personal Care",
    # International beauty brands (from your database)
    "bobbi brown": "Beauty & Personal Care",
    "bobbi brown cosmetics": "Beauty & Personal Care",
    "bobbi brown black friday": "Beauty & Personal Care",
    "kiehl's": "Beauty & Personal Care",
    "kiehl's since 1851": "Beauty & Personal Care",
    "nyx": "Beauty & Personal Care",
    "nyx professional makeup": "Beauty & Personal Care",
    "urban decay": "Beauty & Personal Care",
    "mac": "Beauty & Personal Care",
    "mac cosmetics": "Beauty & Personal Care",
    "mac lover membership": "Beauty & Personal Care",
    "sephora": "Beauty & Personal Care",
    "innisfree": "Beauty & Personal Care",
    "givenchy": "Beauty & Personal Care",
    "revlon": "Beauty & Personal Care",
    "cerave": "Beauty & Personal Care",
    "estee lauder": "Beauty & Personal Care",
    "clinique": "Beauty & Personal Care",
    "l'oreal": "Beauty & Personal Care",
    "maybelline": "Beauty & Personal Care",
    "charlotte tilbury": "Beauty & Personal Care",
    "fenty beauty": "Beauty & Personal Care",
    "rare beauty": "Beauty & Personal Care",
    "glossier": "Beauty & Personal Care",
    "the ordinary": "Beauty & Personal Care",
    "drunk elephant": "Beauty & Personal Care",
    "tatcha": "Beauty & Personal Care",
    "olaplex": "Beauty & Personal Care",
    "dyson": "Electronics & Tech",  # Hair tools & home tech
    
    # ============ Apparel & Accessories (Women's) ============
    # Indian brands
    "myntra": "General / Department Store",  # Multi-category
    "westside": "Apparel & Accessories",
    "w": "Apparel & Accessories",
    "biba": "Apparel & Accessories",
    "fabindia": "Apparel & Accessories",
    "global desi": "Apparel & Accessories",
    "zivame": "Apparel & Accessories",
    "clovia": "Apparel & Accessories",
    "shein": "Apparel & Accessories",
    "urbanic": "Apparel & Accessories",
    "stalkbuylove": "Apparel & Accessories",
    "faballey": "Apparel & Accessories",
    "libas": "Apparel & Accessories",
    "nicobar": "Apparel & Accessories",
    "11.11": "Apparel & Accessories",
    "11.11 / eleven eleven": "Apparel & Accessories",
    "peeli dori": "Apparel & Accessories",
    "ogaan": "Apparel & Accessories",
    "tilfi": "Apparel & Accessories",
    "manish malhotra": "Apparel & Accessories",
    "tribe amrapali": "Apparel & Accessories",
    "shop lune": "Apparel & Accessories",
    "and": "Apparel & Accessories",
    "no nasties": "Apparel & Accessories",
    "truth be told": "Apparel & Accessories",
    "turn black": "Apparel & Accessories",
    # International fashion brands
    "ganni": "Apparel & Accessories",
    "reformation": "Apparel & Accessories",
    "luisaviaroma": "Apparel & Accessories",
    "mytheresa": "Apparel & Accessories",
    "anthropologie": "Apparel & Accessories",
    "maeve by anthropologie": "Apparel & Accessories",
    "zara": "Apparel & Accessories",
    "mango": "Apparel & Accessories",
    "mango sale": "Apparel & Accessories",
    "h&m": "Apparel & Accessories",
    "gucci": "Apparel & Accessories",
    "balenciaga": "Apparel & Accessories",
    "net-a-porter": "Apparel & Accessories",
    "farfetch": "Apparel & Accessories",
    "revolve": "Apparel & Accessories",
    "asos": "Apparel & Accessories",
    "free people": "Apparel & Accessories",
    "cos": "Apparel & Accessories",
    "& other stories": "Apparel & Accessories",
    "everlane": "Apparel & Accessories",
    "realisation par": "Apparel & Accessories",
    "house of cb": "Apparel & Accessories",
    "princess polly": "Apparel & Accessories",
    
    # ============ Apparel & Accessories (Men's) ============
    "bewakoof": "Apparel & Accessories",
    "the souled store": "Apparel & Accessories",
    "snitch": "Apparel & Accessories",
    "rare rabbit": "Apparel & Accessories",
    "jack & jones": "Apparel & Accessories",
    "levis": "Apparel & Accessories",
    "levi's": "Apparel & Accessories",
    "peter england": "Apparel & Accessories",
    "van heusen": "Apparel & Accessories",
    "louis philippe": "Apparel & Accessories",
    "allen solly": "Apparel & Accessories",
    "uniqlo": "Apparel & Accessories",
    "gap": "Apparel & Accessories",
    "calvin klein": "Apparel & Accessories",
    "calvin klein outlet": "Apparel & Accessories",
    "bombay shirt company": "Apparel & Accessories",
    "march tee": "Apparel & Accessories",
    "tommy hilfiger": "Apparel & Accessories",
    "ralph lauren": "Apparel & Accessories",
    "hugo boss": "Apparel & Accessories",
    "massimo dutti": "Apparel & Accessories",
    "bonobos": "Apparel & Accessories",
    "j.crew": "Apparel & Accessories",
    "banana republic": "Apparel & Accessories",
    "brooks brothers": "Apparel & Accessories",
    
    # ============ Food & Beverage ============
    "zomato": "Food & Beverage",
    "swiggy": "Food & Beverage",
    "bigbasket": "Food & Beverage",
    "grofers": "Food & Beverage",
    "blinkit": "Food & Beverage",
    "zepto": "Food & Beverage",
    "instamart": "Food & Beverage",
    "dunzo": "Food & Beverage",
    "dominos": "Food & Beverage",
    "mcdonalds": "Food & Beverage",
    "burger king": "Food & Beverage",
    "kfc": "Food & Beverage",
    "pizza hut": "Food & Beverage",
    "starbucks": "Food & Beverage",
    "chaayos": "Food & Beverage",
    "blue tokai": "Food & Beverage",
    "sleepy owl": "Food & Beverage",
    "licious": "Food & Beverage",
    "freshmeat": "Food & Beverage",
    "country delight": "Food & Beverage",
    "farmer's dog": "Pets",
    "the farmer's dog": "Pets",
    "native pet": "Pets",  # Pet food
    "matt | the farmer's dog": "Pets",
    
    # ============ Travel & Outdoors ============
    "makemytrip": "Travel & Outdoors",
    "goibibo": "Travel & Outdoors",
    "cleartrip": "Travel & Outdoors",
    "yatra": "Travel & Outdoors",
    "ixigo": "Travel & Outdoors",
    "booking": "Travel & Outdoors",
    "airbnb": "Travel & Outdoors",
    "oyo": "Travel & Outdoors",
    "treebo": "Travel & Outdoors",
    "fabhotels": "Travel & Outdoors",
    "redbus": "Travel & Outdoors",
    "indigo": "Travel & Outdoors",
    "spicejet": "Travel & Outdoors",
    "airindia": "Travel & Outdoors",
    "air india": "Travel & Outdoors",
    "vistara": "Travel & Outdoors",
    "all accor": "Travel & Outdoors",
    "all - accor live limitless": "Travel & Outdoors",
    "accor": "Travel & Outdoors",
    "marriott": "Travel & Outdoors",
    "hilton": "Travel & Outdoors",
    "taj": "Travel & Outdoors",
    "ihg": "Travel & Outdoors",
    "hyatt": "Travel & Outdoors",
    
    # ============ Electronics & Tech ============
    "croma": "Electronics & Tech",
    "reliance digital": "Electronics & Tech",
    "vijay sales": "Electronics & Tech",
    "apple": "Electronics & Tech",
    "samsung": "Electronics & Tech",
    "oneplus": "Electronics & Tech",
    "xiaomi": "Electronics & Tech",
    "realme": "Electronics & Tech",
    "boat": "Electronics & Tech",
    "noise": "Electronics & Tech",
    "fire-boltt": "Electronics & Tech",
    "pebble": "Electronics & Tech",
    "fossil": "Electronics & Tech",  # Watches/wearables
    
    # ============ Home & Living ============
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
    "pottery barn": "Home & Living",
    "pottery barn black friday": "Home & Living",
    "pottery barn cyber monday": "Home & Living",
    "pottery barn design services": "Home & Living",
    "pottery barn sale": "Home & Living",
    "west elm": "Home & Living",
    "williams sonoma": "Home & Living",
    "crate & barrel": "Home & Living",
    "cb2": "Home & Living",
    "house of things": "Home & Living",
    
    # ============ Health, Fitness & Wellness ============
    "pharmeasy": "Health, Fitness & Wellness",
    "netmeds": "Health, Fitness & Wellness",
    "1mg": "Health, Fitness & Wellness",
    "apollo pharmacy": "Health, Fitness & Wellness",
    "apollo 24|7": "Health, Fitness & Wellness",
    "apollo24|7": "Health, Fitness & Wellness",
    "healthkart": "Health, Fitness & Wellness",
    "cult.fit": "Health, Fitness & Wellness",
    "cure.fit": "Health, Fitness & Wellness",
    "practo": "Health, Fitness & Wellness",
    "mfine": "Health, Fitness & Wellness",
    "truweight": "Health, Fitness & Wellness",
    "oziva": "Health, Fitness & Wellness",
    "kapiva": "Health, Fitness & Wellness",
    "ultrahuman": "Health, Fitness & Wellness",
    "ultrahuman cyborg": "Health, Fitness & Wellness",
    "whoop": "Health, Fitness & Wellness",
    "oura": "Health, Fitness & Wellness",
    "fitbit": "Health, Fitness & Wellness",
    
    # ============ Finance & Fintech ============
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
    "scapia": "Finance & Fintech",  # Credit card
    "onecard": "Finance & Fintech",
    "niyo": "Finance & Fintech",
    "uni": "Finance & Fintech",
    
    # ============ Baby & Kids ============
    "firstcry": "Baby & Kids",
    "hopscotch": "Baby & Kids",
    "babyhug": "Baby & Kids",
    "the mom co": "Baby & Kids",
    "mamaearth baby": "Baby & Kids",
    "mothercare": "Baby & Kids",
    "the moms co": "Baby & Kids",
    
    # ============ Apparel & Accessories (Athletic) ============
    "decathlon": "Apparel & Accessories",
    "puma": "Apparel & Accessories",
    "nike": "Apparel & Accessories",
    "adidas": "Apparel & Accessories",
    "reebok": "Apparel & Accessories",
    "asics": "Apparel & Accessories",
    "skechers": "Apparel & Accessories",
    "hrx": "Apparel & Accessories",
    "allbirds": "Apparel & Accessories",  # Sustainable sneakers
    "strava": "Apparel & Accessories",  # Fitness tracking app
    "new balance": "Apparel & Accessories",
    "under armour": "Apparel & Accessories",
    "lululemon": "Apparel & Accessories",
    "gymshark": "Apparel & Accessories",
    "alo yoga": "Apparel & Accessories",
    
    # ============ Jewelry & Accessories ============
    "caratlane": "Apparel & Accessories",  # Jewelry
    "tanishq": "Apparel & Accessories",
    "bluestone": "Apparel & Accessories",
    "melorra": "Apparel & Accessories",
    "candere": "Apparel & Accessories",
    "kalyan jewellers": "Apparel & Accessories",
    "malabar gold": "Apparel & Accessories",
    
    # ============ Entertainment ============
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
    
    # ============ General / Department Store (multi-category) ============
    "flipkart": "General / Department Store",
    "amazon": "General / Department Store",
    "snapdeal": "General / Department Store",
    "meesho": "General / Department Store",
    "tatacliq": "General / Department Store",
    "tata cliq": "General / Department Store",
    "reliance": "General / Department Store",
    "jiomart": "General / Department Store",
    "ajio": "General / Department Store",
    "anubhav barsaiyan": "General / Department Store",  # Likely test/personal

    # ============ Brands added from misclassification fix ============
    # Home & Living
    "burrow": "Home & Living",
    "brooklinen": "Home & Living",
    "stayvista": "Home & Living",  # Vacation homes/stays
    "circus": "Home & Living",  # Circus by Godrej — home/lifestyle

    # Luxury / Multi-category Retail (not purely women's fashion)
    "anthropologie": "General / Department Store",
    "shopbop": "General / Department Store",
    "net-a-porter": "General / Department Store",
    "luisaviaroma": "General / Department Store",
    "shopsimon": "General / Department Store",
    "ganni": "General / Department Store",  # Danish fashion — mixed gender
    "gucci": "Luxury & High-End Goods",  # Luxury — mixed gender
    "balenciaga": "Luxury & High-End Goods",
    "mango": "General / Department Store",  # Fast fashion — mixed gender
    "porter": "General / Department Store",  # Could be logistics or retail

    # Beauty & Personal Care (missed)
    "anastasia beverly hills": "Beauty & Personal Care",

    # Health & Wellness
    "quip": "Health, Fitness & Wellness",  # Oral care

    # Sports & Fitness / Outdoor
    "cotopaxi": "Apparel & Accessories",  # Outdoor gear
    "outdoor voices": "Apparel & Accessories",
    "alo": "Apparel & Accessories",  # Alo Yoga

    # Kids & Baby
    "monica + andy": "Baby & Kids",

    # Basics / Underwear (not gendered)
    "bombas": "General / Department Store",  # Socks/basics
    "meundies": "General / Department Store",  # Underwear basics

    # Pets
    "supertails": "Pets",  # Pet care

    # Travel
    "mokobara": "Apparel & Accessories",  # Luggage/bags brand

    # Logistics / Other
    "shadowfax": "General / Department Store",  # Logistics
    "newaudience": "General / Department Store",
    "cosmos": "General / Department Store",
    "nik sharma": "General / Department Store",  # Marketing personality/newsletter

    # Indian Women's Fashion (correctly classified — adding for normalization)
    "aashni + co": "Apparel & Accessories",
    "aashni": "Apparel & Accessories",
    "khara kapas": "Apparel & Accessories",
    "ka-sha": "Apparel & Accessories",
    "payal singhal": "Apparel & Accessories",
    "jaypore": "Apparel & Accessories",
    "j a y p o r e": "Apparel & Accessories",
    "11.11": "Apparel & Accessories",  # 11.11 / Eleven Eleven
    "eleven eleven": "Apparel & Accessories",
    "alex van divner": "Apparel & Accessories",
    "ashwajeet singh": "Apparel & Accessories",  # Designer
    "rothy's": "Apparel & Accessories",  # Women's shoes
    "maeve": "Apparel & Accessories",  # Anthropologie sub-brand for women
    "anthroliving": "Home & Living",  # Anthropologie home line
    "anthro living": "Home & Living",
    "anthro black friday": "General / Department Store",  # Anthropologie sale — mixed
    "anthro cyber monday": "General / Department Store",
    "anthro new arrivals": "General / Department Store",

    # Food & Beverages (missed)
    "daily harvest": "Food & Beverage",  # Meal delivery
    "teabox": "Food & Beverage",  # Tea brand

    # Health supplements
    "ritual": "Health, Fitness & Wellness",  # Vitamins/supplements

    # Luxury fashion (ungendered → General Retail)
    "versace": "Luxury & High-End Goods",

    # ============ Round 2 fixes — brands still unmapped or wrong ============
    # Eyewear / Accessories
    "warby parker": "Apparel & Accessories",  # Eyewear
    "shinola": "General / Department Store",  # Watches/leather goods/lifestyle

    # Fashion brands that should be General Retail (mixed gender)
    "zara": "General / Department Store",
    "uniqlo": "General / Department Store",  # Mixed gender basics
    "gap": "General / Department Store",
    "calvin klein": "General / Department Store",  # Mixed gender

    # Home & Living (missed)
    "interior define": "Home & Living",  # Custom furniture
    "sleepycat": "Home & Living",  # Mattresses
    "eve sleep": "Home & Living",  # Mattresses
    "ecdb": "Home & Living",

    # Beauty & Personal Care (missed)
    "d'you": "Beauty & Personal Care",  # Skincare
    "ilia": "Beauty & Personal Care",  # Clean beauty
    "madison reed": "Beauty & Personal Care",  # Hair color
    "credo beauty": "Beauty & Personal Care",
    "typsy beauty": "Beauty & Personal Care",
    "nua": "Beauty & Personal Care",  # Women's wellness/period care
    "marie claire": "Apparel & Accessories",  # Fashion magazine/brand
    "aromaworks london": "Beauty & Personal Care",  # Aromatherapy

    # Food & Beverages (missed)
    "native pet": "Food & Beverage",  # Pet food (closest category)
    "farmer's dog": "Food & Beverage",  # Pet food delivery

    # Health & Wellness
    "cava athleisure": "Apparel & Accessories",  # Athleisure
    "zevo insect": "Home & Living",  # Insect control — home product

    # Fashion — correctly Women's Fashion
    "bare necessities": "Apparel & Accessories",  # Lingerie
    "autry": "Apparel & Accessories",  # Sneaker brand
    "proper cloth": "Apparel & Accessories",  # Custom men's shirts
    "la maison goyard": "Luxury & High-End Goods",  # Luxury house
    "ayr": "Apparel & Accessories",  # Women's clothing
    "vibecrafts": "Home & Living",  # Crafts/home decor
    "phool": "Home & Living",  # Incense/fragrance — home

    # People/newsletters — General Retail fallback
    "arman sood": "General / Department Store",
    "pallavi": "General / Department Store",
    "james at radiant": "General / Department Store",
    "kellie hackney": "General / Department Store",
    "maddison cox": "General / Department Store",
    "theo at growthrocks": "General / Department Store",
    "matt | the farmer's dog": "Food & Beverage",

    # SaaS/Tools — classify as General Retail rather than None
    "zendesk": "General / Department Store",
    "your zendesk": "General / Department Store",
    "zendesk sell": "General / Department Store",
    "topconsumerreviews.com": "General / Department Store",
    "gabit": "Electronics & Tech",  # Wearable tech
}


# Industry keywords for content-based classification
# Industry keywords for content-based classification (WEIGHTED)
# Format: {industry: [(keyword, weight), ...]}
# Weight 3 = highly specific (only this industry uses it)
# Weight 2 = moderately specific
# Weight 1 = generic (appears across industries)
# Generic/ambiguous words removed: "dress", "top", "blouse", "sandals", "flats",
# "heels", "order", "wallet", "watch", "tablet", "offer", "booking", "room", etc.
INDUSTRY_KEYWORDS = {
    "Apparel & Accessories": [
        ("saree", 3), ("kurti", 3), ("lehenga", 3), ("salwar", 3),
        ("dupatta", 3), ("ethnic wear", 3), ("women's fashion", 3),
        ("lingerie", 3), ("nightwear", 2), ("western wear", 2),
        ("anarkali", 3), ("palazzo", 3), ("churidar", 3),
        ("men's fashion", 3), ("blazer", 2), ("formal wear", 2),
        ("sherwani", 3), ("trouser", 2), ("polo shirt", 2),
        ("sportswear", 3), ("activewear", 3), ("athleisure", 3),
        ("tracksuit", 3), ("sneakers", 2), ("footwear", 2),
        ("handbag", 3), ("sunglasses", 3), ("eyewear", 2),
        ("jewelry", 3), ("necklace", 3), ("earring", 3), ("bracelet", 3),
        ("swimwear", 3), ("outerwear", 2), ("jacket", 2), ("coat", 2),
        ("denim", 2), ("jeans", 2), ("t-shirt", 1),
    ],
    "Baby & Kids": [
        ("baby", 2), ("kids", 2), ("toddler", 3), ("infant", 3),
        ("newborn", 3), ("diaper", 3), ("feeding", 2), ("stroller", 3),
        ("crib", 3), ("toy", 2), ("kids wear", 3), ("baby clothes", 3),
        ("nursery", 3), ("parenting", 3), ("pregnancy", 3), ("maternity", 3),
    ],
    "Beauty & Personal Care": [
        ("skincare", 3), ("makeup", 3), ("cosmetic", 3), ("lipstick", 3),
        ("mascara", 3), ("foundation", 2), ("serum", 3), ("moisturizer", 3),
        ("sunscreen", 3), ("face wash", 3), ("cleanser", 3), ("toner", 2),
        ("haircare", 3), ("shampoo", 3), ("conditioner", 2), ("hair oil", 3),
        ("perfume", 3), ("fragrance", 2), ("deodorant", 2), ("grooming", 2),
        ("nail polish", 3), ("eye shadow", 3), ("blush", 2), ("concealer", 3),
        ("primer", 2), ("beauty routine", 3), ("glow", 1),
    ],
    "Electronics & Tech": [
        ("smartphone", 3), ("laptop", 3), ("computer", 2), ("earbuds", 3),
        ("headphones", 3), ("speaker", 2), ("smartwatch", 3), ("wearable", 2),
        ("television", 3), ("camera", 2), ("appliance", 2), ("gadget", 3),
        ("charger", 2), ("electronic", 2), ("gaming", 2), ("console", 2),
        ("processor", 3), ("tech", 1), ("smart home", 3),
    ],
    "Entertainment": [
        ("movie", 3), ("film", 2), ("cinema", 3), ("theatre", 3),
        ("concert", 3), ("streaming", 2), ("series", 2), ("episode", 3),
        ("season", 1), ("music", 2), ("playlist", 3), ("podcast", 3),
    ],
    "Finance & Fintech": [
        ("payment", 2), ("transaction", 3), ("transfer", 2), ("upi", 3),
        ("credit card", 3), ("debit card", 3), ("emi", 3), ("loan", 3),
        ("insurance", 3), ("invest", 3), ("mutual fund", 3), ("stock", 2),
        ("trading", 3), ("portfolio", 3), ("bank", 2), ("savings", 2),
        ("deposit", 2), ("recharge", 2), ("cashback", 2),
    ],
    "Food & Beverage": [
        ("restaurant", 3), ("pizza", 3), ("burger", 3), ("biryani", 3),
        ("curry", 2), ("cuisine", 3), ("chef", 2), ("grocery", 3),
        ("vegetables", 2), ("fruits", 2), ("coffee", 2), ("smoothie", 3),
        ("dessert", 2), ("cake", 2), ("meat", 2), ("chicken", 2),
        ("seafood", 3), ("snacks", 2), ("breakfast", 1), ("lunch", 1),
        ("dinner", 1), ("menu", 2), ("recipe", 3), ("delicious", 2),
    ],
    "Health, Fitness & Wellness": [
        ("medicine", 3), ("pharmacy", 3), ("wellness", 2), ("vitamin", 3),
        ("supplement", 3), ("doctor", 2), ("prescription", 3),
        ("workout", 2), ("exercise", 2), ("yoga", 2), ("meditation", 3),
        ("protein", 2), ("nutrition", 3), ("diet", 2), ("weight loss", 3),
        ("immunity", 3), ("ayurveda", 3), ("herbal", 2), ("natural remedy", 3),
        ("fitness tracker", 3), ("gym", 2),
    ],
    "Home & Living": [
        ("furniture", 3), ("sofa", 3), ("mattress", 3), ("pillow", 2),
        ("decor", 2), ("living room", 3), ("bedroom", 2), ("bathroom", 2),
        ("curtain", 3), ("carpet", 3), ("rug", 2), ("lamp", 2),
        ("lighting", 2), ("organizer", 2), ("shelf", 2), ("wardrobe", 2),
        ("kitchenware", 3), ("cookware", 3), ("utensil", 2), ("dinnerware", 3),
    ],
    "Pets": [
        ("pet food", 3), ("dog food", 3), ("cat food", 3), ("pet toy", 3),
        ("pet grooming", 3), ("veterinary", 3), ("pet health", 3),
        ("dog treat", 3), ("cat litter", 3), ("pet supplement", 3),
        ("puppy", 2), ("kitten", 2), ("pet bed", 3), ("leash", 3),
    ],
    "Travel & Outdoors": [
        ("flight", 3), ("hotel", 3), ("travel", 3), ("trip", 2),
        ("vacation", 3), ("holiday", 2), ("destination", 3), ("airport", 3),
        ("airline", 3), ("resort", 3), ("accommodation", 3),
        ("car rental", 3), ("tour", 2), ("itinerary", 3),
        ("passport", 3), ("visa", 2), ("luggage", 2),
        ("camping", 3), ("hiking", 3), ("outdoor gear", 3),
    ],
    "Tools, Auto & DIY": [
        ("power tool", 3), ("hand tool", 3), ("drill", 3), ("wrench", 3),
        ("automotive", 3), ("car care", 3), ("car wash", 3),
        ("home improvement", 3), ("diy", 2), ("hardware", 2),
        ("lawn mower", 3), ("garden tool", 3),
    ],
}


def extract_industry(brand_name, subject=None, preview=None, html=None, db_session=None, use_ai=False, return_dict=False):
    """
    Extract industry using keyword-based classification (no AI by default).

    Priority order:
    1. Exact brand name match in mapping
    2. Partial/fuzzy brand name match
    3. Content-based keyword analysis

    Args:
        brand_name: The brand name to classify
        subject: Email subject line
        preview: Email preview text
        html: Full HTML content
        db_session: SQLAlchemy session for caching (optional)
        use_ai: Whether to use AI classification (default False)
        return_dict: If True, return {"industry": ..., "category": ...} instead of string

    Returns:
        Industry string (or dict if return_dict=True), or None
    """
    def _result(industry, category=None):
        if return_dict:
            return {"industry": industry, "category": category}
        return industry
    if not brand_name or brand_name == "Unknown":
        kw_industry = _extract_industry_by_keywords(subject, preview, html)
        return _result(kw_industry)

    # Normalize brand name — strip suffixes like "- A Tata Product", "Sale", etc.
    brand_lower = normalize_brand_name(brand_name)
    # Also create a version without special chars for fuzzy matching
    brand_normalized = re.sub(r'[^a-z0-9\s]', '', brand_lower)

    # Method 1: Exact match in mapping (using normalized name)
    if brand_lower in BRAND_INDUSTRY_MAPPING:
        industry = BRAND_INDUSTRY_MAPPING[brand_lower]
        if db_session:
            _cache_brand_classification(db_session, brand_name, industry, "keyword", 1.0)
        return _result(industry)

    # Method 2: Partial/fuzzy match in mapping
    best_match = None
    best_match_len = 0

    for key, industry in BRAND_INDUSTRY_MAPPING.items():
        key_normalized = re.sub(r'[^a-z0-9\s]', '', key)

        # Exact normalized match
        if key_normalized == brand_normalized:
            best_match = industry
            best_match_len = len(key)
            break

        # Key is substring of brand (e.g., "bobbi brown" in "bobbi brown cosmetics")
        if key_normalized in brand_normalized and len(key) > best_match_len:
            best_match = industry
            best_match_len = len(key)

        # Brand is substring of key
        elif brand_normalized in key_normalized and len(brand_normalized) > 3:
            if len(key) > best_match_len:
                best_match = industry
                best_match_len = len(key)

        # Word-level match (e.g., "kiehl" matches "kiehl's since 1851")
        brand_words = brand_normalized.split()
        key_words = key_normalized.split()
        if any(w in key_words for w in brand_words if len(w) > 3):
            if len(key) > best_match_len:
                best_match = industry
                best_match_len = len(key)

    if best_match:
        if db_session:
            _cache_brand_classification(db_session, brand_name, best_match, "keyword", 0.9)
        return _result(best_match)

    # Method 3: AI classification (only if explicitly enabled)
    if use_ai:
        try:
            from backend.ai_classifier import classify_brand_with_ai, is_ai_available
            if is_ai_available():
                result = classify_brand_with_ai(brand_name, subject, preview)
                industry = result.get("industry")
                subcategory = result.get("subcategory")
                confidence = result.get("confidence", 0.8)

                if db_session and industry:
                    _cache_brand_classification(db_session, brand_name, industry, "ai", confidence)

                return _result(industry, subcategory)
        except Exception as e:
            print(f"AI classification error: {e}")

    # Method 4: Fallback to keyword analysis
    kw_industry = _extract_industry_by_keywords(subject, preview, html)
    return _result(kw_industry)


def _cache_brand_classification(db_session, brand_name: str, industry: str, classified_by: str, confidence: float):
    """Cache a brand classification in the database."""
    try:
        from backend.models import BrandClassification
        
        # Check if already exists
        existing = db_session.query(BrandClassification).filter(
            BrandClassification.brand_name.ilike(brand_name)
        ).first()
        
        if existing:
            # Update if AI classification is more confident or if upgrading from keyword
            if classified_by == "ai" and existing.classified_by != "manual":
                existing.industry = industry
                existing.confidence = confidence
                existing.classified_by = classified_by
        else:
            # Create new
            classification = BrandClassification(
                brand_name=brand_name,
                industry=industry,
                confidence=confidence,
                classified_by=classified_by,
            )
            db_session.add(classification)
        
        db_session.commit()
    except Exception as e:
        print(f"Failed to cache classification: {e}")
        db_session.rollback()


def _extract_industry_by_keywords(subject=None, preview=None, html=None):
    """
    Fallback: Extract industry using weighted keyword analysis.
    Returns None when uncertain rather than guessing wrong.
    """
    MIN_SCORE_THRESHOLD = 4   # Need at least 4 weighted points to classify
    AMBIGUITY_RATIO = 0.7     # If 2nd-best > 70% of best, too ambiguous → None

    # Combine all available text
    text_parts = []
    if subject:
        text_parts.append(subject.lower())
    if preview:
        text_parts.append(preview.lower())
    if html:
        from bs4 import BeautifulSoup
        try:
            soup = BeautifulSoup(html, "html.parser")
            for script in soup(["script", "style"]):
                script.decompose()
            text_parts.append(soup.get_text(separator=" ").lower()[:5000])
        except Exception:
            pass

    if not text_parts:
        return None

    combined_text = " ".join(text_parts)
    subject_lower = subject.lower() if subject else ""

    # Score each industry based on weighted keyword matches
    industry_scores = {}
    for industry, keyword_weights in INDUSTRY_KEYWORDS.items():
        score = 0
        for keyword, weight in keyword_weights:
            if keyword in combined_text:
                score += weight
                # Extra bonus if keyword is in subject (subject is strong signal)
                if subject_lower and keyword in subject_lower:
                    score += weight  # double the weight for subject matches
        if score > 0:
            industry_scores[industry] = score

    if not industry_scores:
        return None

    sorted_scores = sorted(industry_scores.items(), key=lambda x: x[1], reverse=True)
    best_industry, best_score = sorted_scores[0]

    # Not enough signal — return None instead of guessing
    if best_score < MIN_SCORE_THRESHOLD:
        return None

    # Too ambiguous — two industries score similarly
    if len(sorted_scores) > 1:
        second_score = sorted_scores[1][1]
        if second_score / best_score > AMBIGUITY_RATIO:
            return None

    return best_industry


# Campaign type keywords for classification
# Priority order matters - more specific types should be checked first
CAMPAIGN_TYPE_KEYWORDS = {
    # High-priority specific types (check first)
    "Sale": [
        "sale", "discount", "% off", "percent off", "off your", "offer", "deal", "save",
        "clearance", "flash sale", "limited time", "price drop", "markdown",
        "bogo", "buy one get", "extra off", "special offer", "promo", "promotion",
        "50% off", "40% off", "30% off", "20% off", "60% off", "70% off",
        "half off", "up to off", "flat off", "get off", "take off",
        "lowest price", "best price", "reduced", "savings", "bargain",
        "black friday", "cyber monday", "end of season", "final sale",
        "last chance", "ending soon", "hurry", "don't miss", "act now",
        "exclusive offer", "member offer", "special deal", "hot deal",
    ],
    "Welcome": [
        "welcome", "thanks for signing", "thanks for joining", "thank you for subscribing",
        "glad you're here", "nice to meet", "get started", "first order",
        "new member", "welcome aboard", "joined", "you're in", "you are in",
        "welcome to the", "happy to have you", "great to have you",
    ],
    "Abandoned Cart": [
        "forgot something", "left behind", "still in your cart", "waiting for you",
        "complete your order", "finish your purchase", "cart reminder", "items waiting",
        "don't miss out on", "still interested", "your cart", "in your bag",
        "complete checkout", "left in cart", "abandoned", "come back to",
    ],
    "Order Update": [
        "order confirmed", "shipped", "out for delivery", "delivered", "tracking",
        "order status", "shipment", "dispatch", "on its way", "delivery update",
        "your order", "order #", "order number", "shipping update", "package",
        "estimated delivery", "arriving", "in transit", "picked up",
    ],
    "Back in Stock": [
        "back in stock", "restocked", "available again", "they're back",
        "it's back", "now available", "restock", "selling fast", "limited stock",
        "almost gone", "low stock", "few left", "last few", "going fast",
    ],
    "New Arrival": [
        "new arrival", "just landed", "just dropped", "new collection", "new launch",
        "introducing", "meet the", "fresh drop", "just in", "new season",
        "launching", "debut", "brand new", "first look", "sneak peek",
        "preview", "coming soon", "new in", "newly added", "fresh arrival",
        "latest collection", "spring collection", "summer collection",
        "fall collection", "winter collection", "ss24", "aw24", "fw24",
    ],
    "Re-engagement": [
        "miss you", "we miss you", "come back", "haven't seen you", "it's been a while",
        "where have you been", "still there", "reconnect", "checking in",
        "been a while", "long time", "remember us", "we noticed",
    ],
    "Festive": [
        "diwali", "holi", "christmas", "new year", "eid", "rakhi", "pongal",
        "onam", "navratri", "durga puja", "festival", "festive", "celebration",
        "valentine", "valentine's", "be mine", "mother's day", "father's day",
        "thanksgiving", "independence day", "republic day", "easter",
        "gifting", "gift guide", "holiday", "seasonal", "raksha bandhan",
    ],
    "Loyalty": [
        "points", "rewards", "loyalty", "member exclusive", "vip", "tier",
        "cashback", "earn", "redeem", "exclusive access", "insider",
        "members only", "member benefits", "loyalty program", "rewards program",
    ],
    "Confirmation": [
        "confirm your", "confirm subscription", "verify your", "verification",
        "double opt", "confirm you want", "please confirm", "activate your",
        "complete your registration", "verify email", "confirm email",
    ],
    "Feedback": [
        "review", "feedback", "rate us", "how was", "tell us", "survey",
        "share your experience", "your opinion", "rate your", "write a review",
        "leave a review", "your feedback", "quick survey", "take our survey",
    ],
    "Educational": [
        "tips", "how to", "guide", "tutorial", "learn", "discover",
        "skincare routine", "styling tips", "beauty tips", "did you know",
        "pro tip", "expert advice", "the secret", "secrets of", "masterclass",
        "101", "basics", "essentials", "everything you need to know",
    ],
    "Newsletter": [
        "newsletter", "weekly update", "monthly update", "digest", "roundup",
        "this week", "this month", "trending", "what's new", "news from",
        "latest from", "highlights", "weekly picks", "editor's picks",
        "curated for you", "hand-picked", "top stories", "in the news",
    ],
    # Lower priority - catch-all for promotional content
    "Product Showcase": [
        "shop now", "shop the", "explore", "discover", "check out",
        "featured", "spotlight", "collection", "lookbook", "look book",
        "style", "outfit", "wear", "perfect for", "made for",
        "designed for", "crafted", "artisan", "handmade", "luxury",
        "premium", "exclusive", "limited edition", "must-have", "essential",
        "favorite", "favourite", "bestseller", "best seller", "popular",
        "trending now", "top picks", "editor's choice", "staff picks",
        "the new", "all new", "iconic", "classic", "timeless",
    ],
    "Promotional": [
        # This is the catch-all for general marketing emails
        "shop", "buy", "order", "get yours", "available now",
        "free shipping", "free delivery", "complimentary", "gift with purchase",
        "bundle", "set", "kit", "value pack", "combo",
        "upgrade", "enhance", "elevate", "transform", "refresh",
        "your", "you'll love", "just for you", "made for you",
        "attention", "detail", "quality", "craftsmanship",
    ],
}


def extract_campaign_type(subject=None, preview=None, html=None, brand_name=None, use_ai=True):
    """
    Detect the campaign type based on email content.
    Uses keyword analysis first (fast), falls back to AI if uncertain.
    
    Args:
        subject: Email subject line
        preview: Email preview text
        html: Full HTML content
        brand_name: Brand name (helps AI context)
        use_ai: Whether to use AI classification if keywords uncertain
    
    Returns:
        Campaign type string or None
    """
    # First try keyword-based classification (fast, no API call)
    keyword_result = _extract_campaign_type_by_keywords(subject, preview, html)
    
    # If we got a confident keyword match, use it
    if keyword_result:
        return keyword_result
    
    # If AI is enabled and we have a subject, try AI classification
    if use_ai and subject:
        try:
            from backend.ai_classifier import classify_campaign_type_with_ai, is_ai_available
            if is_ai_available():
                result = classify_campaign_type_with_ai(subject, preview, brand_name)
                return result.get("campaign_type")
        except Exception as e:
            print(f"AI campaign classification error: {e}")
    
    return None


def _extract_campaign_type_by_keywords(subject=None, preview=None, html=None):
    """
    Extract campaign type using keyword analysis with improved matching.
    Uses regex patterns for percentages and prioritized scoring.
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
    
    # Early check for percentage patterns in subject (strong Sale indicator)
    if subject:
        subject_lower = subject.lower()
        # Match patterns like "50%", "up to 70%", "flat 30% off"
        if re.search(r'\d+\s*%', subject_lower):
            # Check it's not a review percentage or similar
            if any(word in subject_lower for word in ['off', 'save', 'discount', 'sale', 'deal']):
                return "Sale"
            # Even without explicit sale words, percentage in subject is usually a sale
            if not any(word in subject_lower for word in ['review', 'rating', 'score', 'complete']):
                return "Sale"
    
    if not text_parts:
        return None
    
    # Priority order for campaign types (more specific first)
    # Lower priority types only win if they have significantly higher scores
    priority_types = [
        "Confirmation",      # Very specific - email confirmations
        "Order Update",      # Very specific - transactional
        "Abandoned Cart",    # Very specific - cart reminders
        "Welcome",           # Very specific - onboarding
        "Feedback",          # Very specific - surveys/reviews
        "Back in Stock",     # Specific - inventory
        "Re-engagement",     # Specific - win-back
        "Sale",              # Common but specific intent
        "Festive",           # Seasonal/holiday
        "Loyalty",           # Rewards/points
        "New Arrival",       # Product launches
        "Educational",       # Tips/guides
        "Newsletter",        # Regular updates
        "Product Showcase",  # Product features
        "Promotional",       # Catch-all marketing
    ]
    
    # Score each campaign type
    type_scores = {}
    for campaign_type, keywords in CAMPAIGN_TYPE_KEYWORDS.items():
        score = 0
        matched_keywords = []
        for source, text in text_parts:
            for keyword in keywords:
                if keyword in text:
                    # Weight by source (subject > preview > html)
                    if source == "subject":
                        score += 5
                        matched_keywords.append(f"{keyword}(subj)")
                    elif source == "preview":
                        score += 2
                        matched_keywords.append(f"{keyword}(prev)")
                    else:
                        score += 1
        if score > 0:
            type_scores[campaign_type] = score
    
    if not type_scores:
        return None
    
    # Find the best type considering both score and priority
    best_type = None
    best_score = 0
    
    for campaign_type in priority_types:
        if campaign_type in type_scores:
            score = type_scores[campaign_type]
            # High-priority types need lower threshold
            if campaign_type in ["Confirmation", "Order Update", "Abandoned Cart", "Welcome", "Feedback"]:
                threshold = 2  # Lower threshold for specific types
            elif campaign_type in ["Sale", "Back in Stock", "Re-engagement", "Festive"]:
                threshold = 3
            elif campaign_type in ["Promotional", "Product Showcase"]:
                threshold = 5  # Higher threshold for catch-all types
            else:
                threshold = 3
            
            # Take this type if it meets threshold and is higher priority than current best
            if score >= threshold:
                if best_type is None:
                    best_type = campaign_type
                    best_score = score
                # If current best is a low-priority type, prefer higher-priority even with lower score
                elif priority_types.index(campaign_type) < priority_types.index(best_type):
                    # But only if score is at least half of best score
                    if score >= best_score * 0.5:
                        best_type = campaign_type
                        best_score = score
                # If same priority tier, take higher score
                elif score > best_score:
                    best_type = campaign_type
                    best_score = score
    
    return best_type


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
        try:
            results = service.users().messages().list(
                userId="me",
                labelIds=[label_id],
                maxResults=min(max_results, 500),  # Gmail API max is 500 per page
                pageToken=page_token
            ).execute()
        except Exception as e:
            print(f">>> Page {page_count} failed: {e}")
            print(f">>> Continuing with {len(all_messages)} messages collected so far")
            break

        messages = results.get("messages", [])
        all_messages.extend(messages)
        print(f">>> Page {page_count}: fetched {len(messages)} messages (total so far: {len(all_messages)})")

        # Check if we should continue pagination
        page_token = results.get("nextPageToken")
        if not fetch_all or not page_token:
            break
    
    print(f">>> Total messages to process: {len(all_messages)}")

    records = []

    errors_count = 0
    for idx, msg in enumerate(all_messages):
        msg_id = msg["id"]
        if msg_id in processed:
            continue

        # Progress indicator
        if (idx + 1) % 50 == 0:
            print(f">>> Processing email {idx + 1}/{len(all_messages)}...")

        try:
            message = service.users().messages().get(
                userId="me",
                id=msg_id,
                format="full"
            ).execute()
        except Exception as e:
            errors_count += 1
            print(f">>> Warning: Failed to fetch message {msg_id}: {e}")
            # Mark as processed to skip it next time
            save_processed_id(msg_id)
            if errors_count > 10:
                print(f">>> Too many errors ({errors_count}), stopping early")
                break
            continue

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

    if errors_count > 0:
        print(f">>> Completed with {errors_count} errors (skipped)")
    
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
