"""
Apply brand → (industry, subcategory) classifications directly.
Classified by Claude — no API calls needed.

Usage:
    python apply_classifications.py --dry-run   # Preview changes
    python apply_classifications.py              # Apply to local DB
    DATABASE_URL=postgresql://... python apply_classifications.py  # Production
"""

import sys
from backend.db import SessionLocal, engine, Base
from backend.models import Email, BrandClassification
from sqlalchemy import func

# Brand → (industry, subcategory) mapping
# Classified manually for all brands in the database
BRAND_CLASSIFICATIONS = {
    # === Luxury & High-End Goods ===
    "Net-A-Porter":                ("Luxury & High-End Goods", "Designer Fashion"),
    "Net-A-Porter Sale":           ("Luxury & High-End Goods", "Designer Fashion"),
    "Net-A-Porter Rewards":        ("Luxury & High-End Goods", "Designer Fashion"),
    "Net-A-Porter Promotion":      ("Luxury & High-End Goods", "Designer Fashion"),
    "Net-A-Porter Promotions":     ("Luxury & High-End Goods", "Designer Fashion"),
    "Net A Porter Rewards":        ("Luxury & High-End Goods", "Designer Fashion"),
    "Mytheresa":                   ("Luxury & High-End Goods", "Designer Fashion"),
    "Luisaviaroma":                ("Luxury & High-End Goods", "Designer Fashion"),
    "Balenciaga":                  ("Luxury & High-End Goods", "Designer Fashion"),
    "Gucci":                       ("Luxury & High-End Goods", "Designer Fashion"),
    "Versace":                     ("Luxury & High-End Goods", "Designer Fashion"),
    "Shopbop":                     ("Luxury & High-End Goods", "Designer Fashion"),

    # === Apparel & Accessories — Women's Clothing ===
    "Nicobar":                     ("Apparel & Accessories", "Women's Clothing"),
    "nicobar":                     ("Apparel & Accessories", "Women's Clothing"),
    "Reformation":                 ("Apparel & Accessories", "Women's Clothing"),
    "Shop Lune":                   ("Apparel & Accessories", "Women's Clothing"),
    "No Nasties":                  ("Apparel & Accessories", "Women's Clothing"),
    "Peeli Dori":                  ("Apparel & Accessories", "Women's Clothing"),
    "Aashni + Co":                 ("Apparel & Accessories", "Women's Clothing"),
    "11.11 / Eleven Eleven":       ("Apparel & Accessories", "Women's Clothing"),
    "11-11":                       ("Apparel & Accessories", "Women's Clothing"),
    "Ka-Sha":                      ("Apparel & Accessories", "Women's Clothing"),
    "Payal Singhal":               ("Apparel & Accessories", "Women's Clothing"),
    "Ogaan":                       ("Apparel & Accessories", "Women's Clothing"),
    "Manish Malhotra":             ("Apparel & Accessories", "Women's Clothing"),
    "Khara Kapas":                 ("Apparel & Accessories", "Women's Clothing"),
    "Tilfi Banaras":               ("Apparel & Accessories", "Women's Clothing"),
    "Tilfi":                       ("Apparel & Accessories", "Women's Clothing"),
    "Turn Black":                  ("Apparel & Accessories", "Women's Clothing"),
    "Truth Be Told":               ("Apparel & Accessories", "Women's Clothing"),
    "Ganni":                       ("Apparel & Accessories", "Women's Clothing"),
    "Mango":                       ("Apparel & Accessories", "Women's Clothing"),
    "Mango Sale":                  ("Apparel & Accessories", "Women's Clothing"),
    "Zara":                        ("Apparel & Accessories", "Women's Clothing"),
    "Maeve By Anthropologie":      ("Apparel & Accessories", "Women's Clothing"),
    "Alex Van Divner":             ("Apparel & Accessories", "Women's Clothing"),
    "And":                         ("Apparel & Accessories", "Women's Clothing"),
    "Ayr":                         ("Apparel & Accessories", "Women's Clothing"),
    "J A Y P O R E":              ("Apparel & Accessories", "Women's Clothing"),
    "House Of Masaba":             ("Apparel & Accessories", "Women's Clothing"),

    # === Apparel & Accessories — Men's Clothing ===
    "March Tee":                   ("Apparel & Accessories", "Men's Clothing"),
    "Bombay Shirt Company":        ("Apparel & Accessories", "Men's Clothing"),
    "Proper Cloth":                ("Apparel & Accessories", "Men's Clothing"),
    "Ashwajeet Singh":             ("Apparel & Accessories", "Men's Clothing"),

    # === Apparel & Accessories — Unisex / Gender-Neutral ===
    "Uniqlo":                      ("Apparel & Accessories", "Unisex / Gender-Neutral Clothing"),
    "Calvin Klein":                ("Apparel & Accessories", "Unisex / Gender-Neutral Clothing"),
    "Calvin Klein Outlet":         ("Apparel & Accessories", "Unisex / Gender-Neutral Clothing"),
    "Gap":                         ("Apparel & Accessories", "Unisex / Gender-Neutral Clothing"),
    "Gap App Exclusive":           ("Apparel & Accessories", "Unisex / Gender-Neutral Clothing"),
    "Gapjeans Event":              ("Apparel & Accessories", "Unisex / Gender-Neutral Clothing"),
    "Bombas":                      ("Apparel & Accessories", "Unisex / Gender-Neutral Clothing"),

    # === Apparel & Accessories — Intimates / Lingerie ===
    "Meundies":                    ("Apparel & Accessories", "Intimates / Lingerie"),

    # === Apparel & Accessories — Jewelry ===
    "Caratlane, A Tanishq Partnership": ("Apparel & Accessories", "Jewelry"),
    "Caratlane – A Tata Product":  ("Apparel & Accessories", "Jewelry"),
    "Caratlane - A Tata Product":  ("Apparel & Accessories", "Jewelry"),
    "Caratlane_A Tata Product":    ("Apparel & Accessories", "Jewelry"),
    "Caratlane":                   ("Apparel & Accessories", "Jewelry"),
    "Tribe Amrapali":              ("Apparel & Accessories", "Jewelry"),
    "Tribe Amarpali":              ("Apparel & Accessories", "Jewelry"),

    # === Apparel & Accessories — Footwear ===
    "Rothy'S":                     ("Apparel & Accessories", "Footwear"),
    "Allbirds":                    ("Apparel & Accessories", "Footwear"),

    # === Apparel & Accessories — Activewear / Athleisure ===
    "Outdoor Voices":              ("Apparel & Accessories", "Activewear / Athleisure"),
    "Alo":                         ("Apparel & Accessories", "Activewear / Athleisure"),
    "Cava Athleisure":             ("Apparel & Accessories", "Activewear / Athleisure"),

    # === Apparel & Accessories — Watches ===
    "Fossil":                      ("Apparel & Accessories", "Watches"),

    # === Apparel & Accessories — Sunglasses & Eyewear ===
    "Warby Parker":                ("Apparel & Accessories", "Sunglasses & Eyewear"),

    # === Beauty & Personal Care ===
    "Bobbi Brown Cosmetics":       ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Bobbi Brown Black Friday":    ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Nykaa":                       ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Nyx Professional Makeup":     ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Urban Decay":                 ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Givenchy":                    ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Mac Cosmetics":               ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Mac Lover Membership":        ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Anastasia Beverly Hills":     ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Sephora":                     ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Sephora Sale":                ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Revlon":                      ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Ilia":                        ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Typsy Beauty":                ("Beauty & Personal Care", "Makeup / Cosmetics"),
    "Kiehl'S Since 1851":         ("Beauty & Personal Care", "Skincare"),
    "Innisfree":                   ("Beauty & Personal Care", "Skincare"),
    "Cerave":                      ("Beauty & Personal Care", "Skincare"),
    "Dot & Key":                   ("Beauty & Personal Care", "Skincare"),
    "D'You":                       ("Beauty & Personal Care", "Skincare"),
    "Forest Essentials":           ("Beauty & Personal Care", "Skincare"),
    "Foxtale":                     ("Beauty & Personal Care", "Skincare"),
    "K-Beauty Skin – Authentic Korean Skincare & Beauty Products": ("Beauty & Personal Care", "Skincare"),
    "Madison Reed":                ("Beauty & Personal Care", "Haircare"),
    "Nua":                         ("Beauty & Personal Care", "Others"),

    # === Food & Beverage ===
    "Zomato":                      ("Food & Beverage", "Others"),
    "Swiggy":                      ("Food & Beverage", "Others"),
    "Daily Harvest":               ("Food & Beverage", "Meal Kits"),
    "Rachel At Daily Harvest":     ("Food & Beverage", "Meal Kits"),
    "Sleepy Owl Coffee":           ("Food & Beverage", "Beverages (Coffee, Tea, Juices)"),
    "Teabox":                      ("Food & Beverage", "Beverages (Coffee, Tea, Juices)"),

    # === Pets ===
    "Supertails":                  ("Pets", "Others"),
    "Supertails+ Clinic":          ("Pets", "Pet Health / Supplements"),
    "Farmer'S Dog":                ("Pets", "Pet Food"),
    "Native Pet":                  ("Pets", "Pet Food"),
    "Matt | The Farmer'S Dog":     ("Pets", "Pet Food"),

    # === Home & Living ===
    "Burrow":                      ("Home & Living", "Furniture"),
    "Interior Define":             ("Home & Living", "Furniture"),
    "Pottery Barn":                ("Home & Living", "Home Décor"),
    "Pottery Barn Sale":           ("Home & Living", "Home Décor"),
    "Pottery Barn Design Services":("Home & Living", "Home Décor"),
    "Pottery Barn Black Friday":   ("Home & Living", "Home Décor"),
    "Pottery Barn Cyber Monday":   ("Home & Living", "Home Décor"),
    "House Of Things":             ("Home & Living", "Home Décor"),
    "Address Home":                ("Home & Living", "Home Décor"),
    "Vibecrafts":                  ("Home & Living", "Home Décor"),
    "Phool":                       ("Home & Living", "Home Décor"),
    "Circus A Godrej Enterprises Brand": ("Home & Living", "Home Décor"),
    "Anthroliving":                ("Home & Living", "Home Décor"),
    "Brooklinen":                  ("Home & Living", "Bedding & Bath"),
    "Maeve At Brooklinen":         ("Home & Living", "Bedding & Bath"),
    "Sleepycat":                   ("Home & Living", "Bedding & Bath"),
    "Eve Sleep":                   ("Home & Living", "Bedding & Bath"),
    "Zevo Insect":                 ("Home & Living", "Cleaning Supplies"),

    # === Health, Fitness & Wellness ===
    "Ultrahuman":                  ("Health, Fitness & Wellness", "Wearable Fitness Trackers"),
    "Ultrahuman Cyborg":           ("Health, Fitness & Wellness", "Wearable Fitness Trackers"),
    "Strava":                      ("Health, Fitness & Wellness", "Wearable Fitness Trackers"),
    "Apollo 24|7":                 ("Health, Fitness & Wellness", "Personal Health Devices"),
    "Apollo24|7":                  ("Health, Fitness & Wellness", "Personal Health Devices"),
    "Ritual":                      ("Health, Fitness & Wellness", "Vitamins & Nutrition"),
    "Quip":                        ("Beauty & Personal Care", "Oral Care"),

    # === Electronics & Tech ===
    "Boat Lifestyle":              ("Electronics & Tech", "Headphones & Audio Gear"),
    "Gabit":                       ("Electronics & Tech", "Smartwatches & Wearables"),

    # === Travel & Outdoors ===
    "All - Accor Live Limitless":  ("Travel & Outdoors", "Others"),
    "All Accor":                   ("Travel & Outdoors", "Others"),
    "Mokobara":                    ("Travel & Outdoors", "Luggage & Travel Accessories"),
    "Stayvista":                   ("Travel & Outdoors", "Others"),
    "Cotopaxi":                    ("Travel & Outdoors", "Camping & Hiking Gear"),

    # === Finance & Fintech ===
    "Scapia":                      ("Finance & Fintech", "Credit Cards"),

    # === Baby & Kids ===
    "Monica + Andy":               ("Baby & Kids", "Clothing"),

    # === General / Department Store ===
    "Anthropologie":               ("General / Department Store", "Multi-Category Retail"),
    "Anthropologie Sale":          ("General / Department Store", "Multi-Category Retail"),
    "Anthro Black Friday":         ("General / Department Store", "Multi-Category Retail"),
    "Anthro New Arrivals":         ("General / Department Store", "Multi-Category Retail"),
    "Anthro Cyber Monday":         ("General / Department Store", "Multi-Category Retail"),
    "AJIO":                        ("General / Department Store", "Online Marketplaces"),
    "Reliance":                    ("General / Department Store", "Multi-Category Retail"),
    "Shopsimon":                   ("General / Department Store", "Flash Sale Retailers"),
    "Porter":                      ("General / Department Store", "Others"),
    "Shadowfax":                   ("General / Department Store", "Others"),
    "Cosmos":                      ("General / Department Store", "Others"),
    "Anubhav Barsaiyan":           ("General / Department Store", "Others"),

    # === Business & B2B Retail ===
    "Nik Sharma":                  ("Business & B2B Retail", "Others"),
    "Newaudience":                 ("Business & B2B Retail", "Others"),
    "Zendesk":                     ("Business & B2B Retail", "Others"),
    "Zendesk Sell":                ("Business & B2B Retail", "Others"),
    "Your Zendesk":                ("Business & B2B Retail", "Others"),
    "What'S New At Zendesk":       ("Business & B2B Retail", "Others"),
    "Theo At Growthrocks":         ("Business & B2B Retail", "Others"),

    # === Junk / Unknown — default to General ===
    "email":                       ("General / Department Store", "Others"),
    "em":                          ("General / Department Store", "Others"),
    "mail":                        ("General / Department Store", "Others"),
    "e":                           ("General / Department Store", "Others"),
    "emails":                      ("General / Department Store", "Others"),
    "reply":                       ("General / Department Store", "Others"),
    "store":                       ("General / Department Store", "Others"),
    "154414822":                   ("General / Department Store", "Others"),
    "Topconsumerreviews.Com":      ("General / Department Store", "Others"),
    "Arman Sood":                  ("General / Department Store", "Others"),
    "Maddison Cox":                ("General / Department Store", "Others"),
    "Pallavi":                     ("General / Department Store", "Others"),
    "James At Radiant":            ("General / Department Store", "Others"),
    "Kellie Hackney":              ("General / Department Store", "Others"),
}


def apply(db, dry_run=False):
    # Show current state
    results = (
        db.query(Email.industry, func.count(Email.id))
        .group_by(Email.industry)
        .order_by(func.count(Email.id).desc())
        .all()
    )
    print("--- Before ---")
    total = sum(r[1] for r in results)
    for industry, count in results:
        print(f"  {count:>5} ({count/total*100:5.1f}%)  {industry}")
    print(f"  {'─'*35}\n  {total:>5}         Total\n")

    # Apply classifications
    total_updated = 0
    changes = []

    for brand, (new_industry, new_subcategory) in sorted(BRAND_CLASSIFICATIONS.items()):
        # Count emails that need updating
        from sqlalchemy import or_, and_
        needs_update = db.query(Email).filter(
            Email.brand == brand,
            or_(
                Email.industry != new_industry,
                Email.industry.is_(None),
                Email.category != new_subcategory,
                Email.category.is_(None),
            )
        ).count()

        if needs_update > 0:
            current = db.query(Email.industry, Email.category).filter(Email.brand == brand).first()
            cur_ind = current.industry if current else "None"
            cur_cat = current.category if current else "None"

            changes.append((brand, cur_ind, cur_cat, new_industry, new_subcategory, needs_update))

            ind_marker = "≠" if cur_ind != new_industry else "="
            print(f"  {ind_marker} {brand:45s} | {cur_ind or 'None':35s} → {new_industry:30s} | {new_subcategory:35s} | {needs_update} emails")

            if not dry_run:
                db.query(Email).filter(Email.brand == brand).update({
                    "industry": new_industry,
                    "category": new_subcategory,
                })

                # Update brand_classifications cache
                existing = db.query(BrandClassification).filter(
                    BrandClassification.brand_name.ilike(brand)
                ).first()
                if existing:
                    existing.industry = new_industry
                    existing.confidence = 1.0
                    existing.classified_by = "manual"
                else:
                    db.add(BrandClassification(
                        brand_name=brand,
                        industry=new_industry,
                        confidence=1.0,
                        classified_by="manual",
                    ))

            total_updated += needs_update

    if not dry_run and total_updated > 0:
        db.commit()

    print(f"\n--- Summary ---")
    print(f"  Brands updated: {len(changes)}")
    print(f"  Emails updated: {total_updated}")

    if dry_run:
        print(f"\n  [DRY RUN] No changes applied.")
    else:
        # Show after state
        results = (
            db.query(Email.industry, func.count(Email.id))
            .group_by(Email.industry)
            .order_by(func.count(Email.id).desc())
            .all()
        )
        print("\n--- After ---")
        total = sum(r[1] for r in results)
        for industry, count in results:
            print(f"  {count:>5} ({count/total*100:5.1f}%)  {industry}")
        print(f"  {'─'*35}\n  {total:>5}         Total")

        # Show subcategory distribution
        cat_results = (
            db.query(Email.category, func.count(Email.id))
            .filter(Email.category.isnot(None))
            .group_by(Email.category)
            .order_by(func.count(Email.id).desc())
            .all()
        )
        if cat_results:
            print(f"\n--- Subcategories ---")
            for cat, count in cat_results:
                print(f"  {count:>5}  {cat}")


def main():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()
    try:
        dry_run = "--dry-run" in sys.argv
        apply(db, dry_run=dry_run)
    finally:
        db.close()


if __name__ == "__main__":
    main()
