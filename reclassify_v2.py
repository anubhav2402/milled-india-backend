"""
Reclassify all emails using the new 17-category taxonomy.

Phase A: Deterministic rename of old industry names → new names
Phase B: AI-powered subcategory classification per brand (populates `category` column)

Usage:
    python reclassify_v2.py --dry-run          # Preview all changes
    python reclassify_v2.py --rename-only      # Only do Phase A (deterministic renames)
    python reclassify_v2.py                    # Full run: rename + AI subcategories
    DATABASE_URL=postgresql://... python reclassify_v2.py  # Against production DB
"""

import sys
import time
import json
from collections import defaultdict

from backend.db import SessionLocal, engine, Base
from backend.models import Email, BrandClassification
from backend.ai_classifier import (
    INDUSTRIES,
    SUBCATEGORIES,
    classify_brand_with_ai,
    is_ai_available,
)
from sqlalchemy import func, or_


# Old industry name → new industry name (deterministic mapping)
RENAME_MAP = {
    "Women's Fashion": "Apparel & Accessories",
    "Men's Fashion": "Apparel & Accessories",
    "Food & Beverages": "Food & Beverage",
    "Travel & Hospitality": "Travel & Outdoors",
    "Electronics & Gadgets": "Electronics & Tech",
    "Health & Wellness": "Health, Fitness & Wellness",
    "Kids & Baby": "Baby & Kids",
    "Sports & Fitness": "Apparel & Accessories",
    "General Retail": "General / Department Store",
}


def show_distribution(db, label="Industry Distribution"):
    """Show current industry + category distribution."""
    results = (
        db.query(Email.industry, func.count(Email.id))
        .group_by(Email.industry)
        .order_by(func.count(Email.id).desc())
        .all()
    )
    print(f"\n--- {label} ---")
    total = sum(r[1] for r in results)
    for industry, count in results:
        pct = count / total * 100
        print(f"  {count:>5} ({pct:5.1f}%)  {industry}")
    print(f"  {'─' * 35}")
    print(f"  {total:>5}         Total")

    # Show category distribution (top 20)
    cat_results = (
        db.query(Email.category, func.count(Email.id))
        .filter(Email.category.isnot(None))
        .group_by(Email.category)
        .order_by(func.count(Email.id).desc())
        .limit(20)
        .all()
    )
    if cat_results:
        print(f"\n--- Top Subcategories ---")
        for cat, count in cat_results:
            print(f"  {count:>5}  {cat}")


def phase_a_rename(db, dry_run=False):
    """Phase A: Deterministic rename of old industry names."""
    print("\n=== Phase A: Deterministic Renames ===\n")
    total_renamed = 0

    for old_name, new_name in RENAME_MAP.items():
        count = db.query(Email).filter(Email.industry == old_name).count()
        if count > 0:
            print(f"  {old_name:25s} → {new_name:30s}  ({count} emails)")
            if not dry_run:
                db.query(Email).filter(Email.industry == old_name).update(
                    {"industry": new_name}
                )
            total_renamed += count

    # Also rename any industry values not in the new INDUSTRIES list
    orphan_results = (
        db.query(Email.industry, func.count(Email.id))
        .filter(
            Email.industry.isnot(None),
            ~Email.industry.in_(INDUSTRIES),
        )
        .group_by(Email.industry)
        .all()
    )
    if orphan_results:
        print(f"\n  Orphan industries (not in new taxonomy):")
        for ind, count in orphan_results:
            print(f"    {ind}: {count} emails → will be reclassified by AI in Phase B")

    if not dry_run and total_renamed > 0:
        db.commit()

    print(f"\n  Phase A total: {total_renamed} emails renamed")
    return total_renamed


def phase_b_subcategories(db, dry_run=False):
    """Phase B: AI-powered subcategory classification per brand."""
    print("\n=== Phase B: AI Subcategory Classification ===\n")

    if not is_ai_available():
        print("  ERROR: OPENAI_API_KEY not set. Cannot run Phase B.")
        print("  Set the environment variable and try again.")
        return 0

    # Get all distinct brands with their current industry and email counts
    brand_data = (
        db.query(
            Email.brand,
            Email.industry,
            func.count(Email.id).label("count"),
        )
        .filter(Email.brand.isnot(None))
        .group_by(Email.brand, Email.industry)
        .order_by(func.count(Email.id).desc())
        .all()
    )

    # Group by brand
    brands = defaultdict(lambda: {"industries": set(), "count": 0})
    for brand, industry, count in brand_data:
        brands[brand]["industries"].add(industry)
        brands[brand]["count"] += count

    total_updated = 0
    total_brands = len(brands)
    ai_calls = 0
    disagreements = []

    print(f"  Processing {total_brands} brands...\n")

    for i, (brand, info) in enumerate(sorted(brands.items(), key=lambda x: -x[1]["count"])):
        if not brand or brand.strip() == "":
            continue

        current_industry = next(iter(info["industries"]))  # pick first
        email_count = info["count"]

        # Get a sample email for AI context
        sample = (
            db.query(Email.subject, Email.preview)
            .filter(Email.brand == brand)
            .order_by(Email.received_at.desc())
            .first()
        )

        sample_subject = sample.subject if sample else None
        sample_preview = sample.preview if sample else None

        # Call AI for classification
        try:
            result = classify_brand_with_ai(brand, sample_subject, sample_preview)
            ai_industry = result.get("industry", "General / Department Store")
            ai_subcategory = result.get("subcategory", "Others")
            confidence = result.get("confidence", 0.5)
            ai_calls += 1
        except Exception as e:
            print(f"  ERROR classifying {brand}: {e}")
            continue

        # Check if AI disagrees with current industry
        if ai_industry != current_industry and current_industry in INDUSTRIES:
            disagreements.append({
                "brand": brand,
                "current": current_industry,
                "ai_says": ai_industry,
                "confidence": confidence,
                "emails": email_count,
            })

        # Determine final values
        # If AI has high confidence and disagrees, trust AI
        # Otherwise keep current industry but add subcategory
        if confidence >= 0.8 and ai_industry in INDUSTRIES:
            final_industry = ai_industry
        elif current_industry in INDUSTRIES:
            final_industry = current_industry
        else:
            final_industry = ai_industry

        final_subcategory = ai_subcategory

        # Check what needs updating
        needs_update = False
        updates = {}

        if final_industry != current_industry:
            updates["industry"] = final_industry
            needs_update = True

        # Always set subcategory (it's NULL for all emails currently)
        updates["category"] = final_subcategory
        needs_update = True

        status = "→" if updates.get("industry") else "+"
        ind_change = f"{current_industry} → {final_industry}" if updates.get("industry") else final_industry
        print(f"  [{i+1:3d}/{total_brands}] {status} {brand:35s} | {ind_change:35s} | {final_subcategory:30s} | {email_count} emails | conf={confidence:.2f}")

        if not dry_run and needs_update:
            db.query(Email).filter(Email.brand == brand).update(updates)
            total_updated += email_count

            # Update brand_classifications cache
            existing = db.query(BrandClassification).filter(
                BrandClassification.brand_name.ilike(brand)
            ).first()
            if existing:
                existing.industry = final_industry
                existing.confidence = confidence
                existing.classified_by = "ai"
            else:
                db.add(BrandClassification(
                    brand_name=brand,
                    industry=final_industry,
                    confidence=confidence,
                    classified_by="ai",
                ))

        # Rate limit: 0.5s between API calls
        if ai_calls % 10 == 0 and not dry_run:
            db.commit()  # Commit every 10 brands
        time.sleep(0.5)

    if not dry_run:
        db.commit()

    print(f"\n  Phase B total: {total_updated} emails updated across {ai_calls} AI calls")

    if disagreements:
        print(f"\n  --- AI Disagreements ({len(disagreements)}) ---")
        print(f"  These brands were reclassified (AI confidence >= 0.8):\n")
        for d in sorted(disagreements, key=lambda x: -x["emails"]):
            print(f"    {d['brand']:35s} {d['current']:30s} → {d['ai_says']:30s} (conf={d['confidence']:.2f}, {d['emails']} emails)")

    return total_updated


def main():
    Base.metadata.create_all(bind=engine)
    db = SessionLocal()

    try:
        dry_run = "--dry-run" in sys.argv
        rename_only = "--rename-only" in sys.argv

        if dry_run:
            print("╔═══════════════════════════════════╗")
            print("║         DRY RUN MODE              ║")
            print("║   No changes will be applied      ║")
            print("╚═══════════════════════════════════╝")

        print("\n=== Before Reclassification ===")
        show_distribution(db, "Current Distribution")

        # Phase A: Deterministic renames
        phase_a_count = phase_a_rename(db, dry_run=dry_run)

        if rename_only:
            if not dry_run:
                print("\n=== After Phase A (rename only) ===")
                show_distribution(db)
            print("\nDone (rename only). Run without --rename-only for AI subcategories.")
            return

        # Phase B: AI subcategory classification
        phase_b_count = phase_b_subcategories(db, dry_run=dry_run)

        if dry_run:
            print(f"\n[DRY RUN] Would update {phase_a_count + phase_b_count} emails total.")
            print("Run without --dry-run to apply changes.")
        else:
            print("\n=== After Reclassification ===")
            show_distribution(db)

    finally:
        db.close()


if __name__ == "__main__":
    main()
