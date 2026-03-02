"""
Batch reclassify emails with NULL type using Claude Haiku.
Processes emails in batches of 10 for efficiency.

Usage:
    python reclassify_types.py                              # Dry run
    python reclassify_types.py --apply                      # Apply changes
    ANTHROPIC_API_KEY=sk-... python reclassify_types.py --apply  # With key
"""

import os
import sys
import json
import time
import sqlite3

# Valid campaign types
CAMPAIGN_TYPES = [
    "Sale", "Welcome", "Abandoned Cart", "Newsletter", "New Arrival",
    "Re-engagement", "Order Update", "Festive", "Loyalty", "Feedback",
    "Back in Stock", "Educational", "Product Showcase", "Promotional",
    "Confirmation", "Brand Story", "Event / Invitation", "Referral",
]

BATCH_SIZE = 10
API_DELAY = 0.3  # seconds between API calls


def get_client():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.")
        print("Run with: ANTHROPIC_API_KEY=sk-... python reclassify_types.py")
        sys.exit(1)
    from anthropic import Anthropic
    return Anthropic(api_key=api_key)


def classify_batch(client, emails):
    """
    Classify a batch of emails (list of (id, brand, subject) tuples).
    Returns list of (id, campaign_type) tuples.
    """
    lines = []
    for i, (eid, brand, subject) in enumerate(emails):
        lines.append(f"{i+1}. [{brand or 'Unknown'}] {subject or '(no subject)'}")

    prompt = f"""Classify each of these marketing emails into a campaign type.

{chr(10).join(lines)}

Choose ONLY from these types:
{json.dumps(CAMPAIGN_TYPES)}

Guidelines:
- "Sale" = discounts, % off, deals, clearance, flash sale, end of season
- "New Arrival" = new products, launches, collections, "just dropped", "just landed", "new in"
- "Product Showcase" = featuring specific products, lookbooks, styling, collections without sale angle. DEFAULT for product-focused emails.
- "Brand Story" = brand narrative, behind-the-scenes, designer stories, fashion shows, collaborations, playlists, lifestyle content, brand campaigns
- "Educational" = tips, how-to, guides, blog content, health/wellness advice, routines
- "Event / Invitation" = store events, pop-ups, webinars, "come visit us", "join us"
- "Festive" = holiday-themed (Diwali, Christmas, Valentine's), gift guides
- "Newsletter" = regular digests, roundups, curated picks, "what's new"
- "Loyalty" = points, rewards, VIP, member exclusives
- "Promotional" = general marketing, free shipping, bundles (LAST RESORT only)
- "Referral" = refer-a-friend programs
- "Back in Stock" = restocked, available again, selling fast
- "Welcome" = first email after signup, onboarding
- "Abandoned Cart" = cart reminders
- "Order Update" = shipping, delivery, tracking
- "Feedback" = reviews, surveys
- "Confirmation" = email verification
- "Re-engagement" = win-back, "we miss you"

Default to "Product Showcase" for product-focused emails that don't clearly fit another type.

Respond with ONLY a JSON array, one per email:
[{{"i": 1, "t": "Product Showcase"}}, {{"i": 2, "t": "Brand Story"}}]"""

    try:
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=400,
            system="You are a precise email classifier. Respond with valid JSON only. No explanation.",
            messages=[{"role": "user", "content": prompt}],
            timeout=20,
        )

        result_text = response.content[0].text.strip()

        # Strip markdown fences
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.startswith("json"):
                result_text = result_text[4:]
            result_text = result_text.strip()

        results = json.loads(result_text)

        classified = []
        for item in results:
            idx = item.get("i", item.get("index", 0)) - 1
            ctype = item.get("t", item.get("type", "Promotional"))
            if ctype not in CAMPAIGN_TYPES:
                ctype = "Promotional"
            if 0 <= idx < len(emails):
                classified.append((emails[idx][0], ctype))

        return classified

    except Exception as e:
        print(f"\n  ERROR in batch: {e}")
        return []


def main():
    apply = "--apply" in sys.argv
    db_path = "emails.db"

    if not os.path.exists(db_path):
        print(f"Database not found: {db_path}")
        sys.exit(1)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Get all NULL-type emails
    cursor.execute(
        "SELECT id, brand, subject FROM emails WHERE type IS NULL AND subject IS NOT NULL ORDER BY id"
    )
    null_emails = cursor.fetchall()
    total = len(null_emails)

    print(f"Found {total} emails with NULL type")
    print(f"Mode: {'APPLY' if apply else 'DRY RUN'}")
    print(f"Batches: {(total + BATCH_SIZE - 1) // BATCH_SIZE}")
    print()

    if total == 0:
        print("Nothing to classify!")
        conn.close()
        return

    client = get_client()
    classified_count = 0
    type_counts = {}

    for i in range(0, total, BATCH_SIZE):
        batch = null_emails[i : i + BATCH_SIZE]
        batch_num = i // BATCH_SIZE + 1
        total_batches = (total + BATCH_SIZE - 1) // BATCH_SIZE

        print(f"Batch {batch_num}/{total_batches}:", end="")

        results = classify_batch(client, batch)

        for eid, ctype in results:
            orig = next((b, s) for (id_, b, s) in batch if id_ == eid)
            brand, subject = orig
            print(f"\n  [{brand}] \"{subject[:55]}\" → {ctype}", end="")

            type_counts[ctype] = type_counts.get(ctype, 0) + 1
            classified_count += 1

            if apply:
                cursor.execute(
                    "UPDATE emails SET type = ? WHERE id = ?", (ctype, eid)
                )

        if apply:
            conn.commit()

        print(f"\n  ✓ {len(results)} classified")

        if i + BATCH_SIZE < total:
            time.sleep(API_DELAY)

    print()
    print(f"=== Summary ===")
    print(f"Total classified: {classified_count}/{total}")
    print(f"\nType distribution:")
    for ctype, cnt in sorted(type_counts.items(), key=lambda x: -x[1]):
        print(f"  {ctype}: {cnt}")

    if not apply:
        print()
        print("This was a DRY RUN. Run with --apply to save changes.")

    conn.close()


if __name__ == "__main__":
    main()
