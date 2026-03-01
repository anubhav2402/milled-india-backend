"""
Twitter integration for MailMuse — tweet generation and posting.
Uses tweepy for Twitter API v2 and OpenAI for content generation.
"""

import os
import json
import random
from datetime import datetime, timedelta
from typing import Optional

import tweepy
from sqlalchemy import func
from sqlalchemy.orm import Session

from .db import SessionLocal
from .models import Email


# ---------------------------------------------------------------------------
# Twitter client helpers
# ---------------------------------------------------------------------------

def is_twitter_configured() -> bool:
    """Return True if all four Twitter API environment variables are set."""
    return all(
        os.getenv(var)
        for var in (
            "TWITTER_API_KEY",
            "TWITTER_API_SECRET",
            "TWITTER_ACCESS_TOKEN",
            "TWITTER_ACCESS_TOKEN_SECRET",
        )
    )


def get_twitter_client() -> tweepy.Client:
    """Build and return an authenticated tweepy v2 Client."""
    client = tweepy.Client(
        consumer_key=os.getenv("TWITTER_API_KEY"),
        consumer_secret=os.getenv("TWITTER_API_SECRET"),
        access_token=os.getenv("TWITTER_ACCESS_TOKEN"),
        access_token_secret=os.getenv("TWITTER_ACCESS_TOKEN_SECRET"),
        wait_on_rate_limit=False,
    )
    return client


# ---------------------------------------------------------------------------
# Posting
# ---------------------------------------------------------------------------

def post_tweet(content: str) -> str:
    """
    Post a tweet and return the tweet ID as a string.

    Raises a RuntimeError with a descriptive message on failure.
    """
    if not is_twitter_configured():
        raise RuntimeError("Twitter API credentials are not configured")

    try:
        client = get_twitter_client()
        response = client.create_tweet(text=content)
        tweet_id = str(response.data["id"])
        print(f"Tweet posted successfully — ID {tweet_id}")
        return tweet_id
    except tweepy.TweepyException as exc:
        print(f"Tweepy error while posting tweet: {exc}")
        raise RuntimeError(f"Failed to post tweet: {exc}") from exc
    except Exception as exc:
        print(f"Unexpected error while posting tweet: {exc}")
        raise RuntimeError(f"Failed to post tweet: {exc}") from exc


# ---------------------------------------------------------------------------
# OpenAI helper (mirrors ai_classifier.py pattern)
# ---------------------------------------------------------------------------

def _get_anthropic_client():
    """Get Anthropic client with API key from environment."""
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    from anthropic import Anthropic
    return Anthropic(api_key=api_key)


def _call_claude_for_tweet(system_prompt: str, user_prompt: str) -> str:
    """
    Ask Claude to write a tweet.  Returns the raw text from the model.
    """
    client = _get_anthropic_client()

    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=200,
        system=system_prompt,
        messages=[
            {"role": "user", "content": user_prompt},
        ],
        timeout=25,
    )

    result_text = response.content[0].text.strip()

    # Strip markdown code fences if the model wraps its answer
    if result_text.startswith("```"):
        result_text = result_text.split("```")[1]
        if result_text.startswith("json"):
            result_text = result_text[4:]
        result_text = result_text.strip()

    # If the model returns JSON with a "tweet" key, extract it
    try:
        parsed = json.loads(result_text)
        if isinstance(parsed, dict) and "tweet" in parsed:
            result_text = parsed["tweet"]
    except (json.JSONDecodeError, TypeError):
        pass  # plain text is fine

    return result_text


# ---------------------------------------------------------------------------
# Data queries & tweet generation per type
# ---------------------------------------------------------------------------

SITE_URL = "\nhttps://www.mailmuse.in?ref=twitter"
# Twitter counts URLs as 23 chars (t.co) + 1 for newline = 24.  280 - 24 = 256.
MAX_TWEET_BODY_LEN = 255

SYSTEM_PROMPT = (
    "You are a witty, concise social-media copywriter for MailMuse, "
    "an email marketing intelligence platform tracking 10,000+ brands worldwide. "
    "Write a single tweet (max 270 characters, no hashtags unless they "
    "feel natural). Be insightful, data-driven, and engaging. "
    "Return ONLY the tweet text — no quotes, no labels, no extra formatting."
)

VIRAL_THREAD_SYSTEM_PROMPT = """You are an elite Twitter/X ghostwriter for MailMuse, an email marketing intelligence platform tracking 10,000+ brands.

Your job: turn raw email marketing data into VIRAL Twitter threads that get saves, retweets, and followers.

## VIRAL FRAMEWORKS (use one or combine):

1. **Myth Buster**: "[Common belief] is wrong. Here's what the data actually shows."
   - Works because: challenges tribal knowledge, creates righteous anger
   - Example hook: "Everything you learned about email subject lines is wrong."

2. **Contrarian Take**: "Everyone says [X]. The data says the opposite."
   - Works because: pattern interrupt, makes reader question their assumptions
   - Example: "The most successful email brands never say 'sale.'"

3. **Bold Claim + Proof**: Make a shocking claim in tweet 1, then prove it with receipts.
   - Works because: curiosity gap forces people to read the thread
   - Example: "The best email subject line I found was 3 characters long."

4. **Data-Driven Surprise**: Lead with the most counterintuitive stat.
   - Works because: unexpected data creates "wait, what?" moments
   - Example: "11 brands use ZERO exclamation marks. They include Gucci and Balenciaga."

5. **Good vs Bad Comparison**: Show two brands, same context, opposite approach.
   - Works because: concrete comparison is more memorable than abstract advice

## EMOTIONAL TRIGGERS (thread MUST hit at least 2):
- **Surprise**: Unexpected data that makes people go "wait, really?"
- **Validation**: Makes readers feel smart for already suspecting something
- **Anger/Indignation**: Calls out lazy practices most brands follow
- **FOMO/Save-worthy**: Real examples people will bookmark as a swipe file

## THREAD STRUCTURE RULES:
- Tweet 1 (Hook): Under 110 characters. Create a massive curiosity gap. No hashtags.
- Tweets 2-7: Build the argument with REAL data and REAL brand examples. Each tweet must be under 280 characters.
- Second-to-last tweet: The insight/"so what" — the deeper lesson beyond tactics.
- Last tweet: CTA mentioning MailMuse (mailmuse.in). Keep it under 160 chars.
- Total: 7-9 tweets per thread.

## OUTPUT FORMAT:
Return ONLY the thread tweets, separated by exactly "---" on its own line.
No tweet numbers, no labels, no formatting. Just the raw tweet text between --- separators.
Each tweet MUST be under 280 characters.
Do NOT include any URL in any tweet except the last one which should end with: mailmuse.in"""


def _build_daily_digest(db: Session) -> str:
    """Query today's emails and build a daily-digest tweet."""
    since = datetime.utcnow() - timedelta(hours=24)

    rows = (
        db.query(Email.brand, func.count(Email.id).label("cnt"))
        .filter(Email.received_at >= since, Email.brand.isnot(None))
        .group_by(Email.brand)
        .order_by(func.count(Email.id).desc())
        .limit(5)
        .all()
    )

    if not rows:
        return "No emails tracked in the last 24 hours."

    # Grab a representative subject per brand for extra flavour
    brand_subjects = {}
    for brand, _ in rows:
        email = (
            db.query(Email)
            .filter(Email.brand == brand, Email.received_at >= since)
            .order_by(Email.received_at.desc())
            .first()
        )
        if email:
            brand_subjects[brand] = email.subject[:60]

    lines = []
    for i, (brand, cnt) in enumerate(rows, 1):
        snippet = brand_subjects.get(brand, "")
        lines.append(f"{i}. {brand} ({cnt} emails) — \"{snippet}\"")

    context = (
        "Today's top email campaigns tracked on MailMuse:\n"
        + "\n".join(lines)
    )
    return context


def _build_weekly_digest(db: Session) -> str:
    """Query the last 7 days and build a weekly-digest tweet."""
    since = datetime.utcnow() - timedelta(days=7)

    total_emails = (
        db.query(func.count(Email.id))
        .filter(Email.received_at >= since)
        .scalar()
    ) or 0

    total_brands = (
        db.query(func.count(func.distinct(Email.brand)))
        .filter(Email.received_at >= since, Email.brand.isnot(None))
        .scalar()
    ) or 0

    top_brand_row = (
        db.query(Email.brand, func.count(Email.id).label("cnt"))
        .filter(Email.received_at >= since, Email.brand.isnot(None))
        .group_by(Email.brand)
        .order_by(func.count(Email.id).desc())
        .first()
    )
    top_brand = top_brand_row[0] if top_brand_row else "N/A"

    trending_type_row = (
        db.query(Email.type, func.count(Email.id).label("cnt"))
        .filter(Email.received_at >= since, Email.type.isnot(None))
        .group_by(Email.type)
        .order_by(func.count(Email.id).desc())
        .first()
    )
    trending_type = trending_type_row[0] if trending_type_row else "Newsletter"

    context = (
        f"This week on MailMuse: {total_emails} campaigns tracked from "
        f"{total_brands} brands. Top brand: {top_brand}. "
        f"Trending campaign type: {trending_type}."
    )
    return context


def _build_brand_spotlight(db: Session) -> str:
    """Pick a random brand with 3+ emails in the last 7 days and spotlight it."""
    since = datetime.utcnow() - timedelta(days=7)

    active_brands = (
        db.query(Email.brand, func.count(Email.id).label("cnt"))
        .filter(Email.received_at >= since, Email.brand.isnot(None))
        .group_by(Email.brand)
        .having(func.count(Email.id) >= 3)
        .all()
    )

    if not active_brands:
        return (
            "No brand sent 3 or more emails this week — "
            "not enough data for a spotlight."
        )

    brand, email_count = random.choice(active_brands)

    # Favourite day of week (0 = Monday … 6 = Sunday)
    emails = (
        db.query(Email)
        .filter(Email.brand == brand, Email.received_at >= since)
        .all()
    )

    day_counts: dict[int, int] = {}
    total_subject_len = 0
    for e in emails:
        if e.received_at:
            weekday = e.received_at.weekday()
            day_counts[weekday] = day_counts.get(weekday, 0) + 1
        total_subject_len += len(e.subject) if e.subject else 0

    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    fav_day = day_names[max(day_counts, key=day_counts.get)] if day_counts else "N/A"
    avg_subject_len = round(total_subject_len / len(emails)) if emails else 0

    context = (
        f"Brand spotlight — {brand}:\n"
        f"- Emails this week: {email_count}\n"
        f"- Favourite send day: {fav_day}\n"
        f"- Avg subject-line length: {avg_subject_len} chars\n"
        "Write an insightful tweet about this brand's email strategy."
    )
    return context


def _build_subject_line_insight(db: Session) -> str:
    """Analyse subject-line patterns from the last 7 days."""
    since = datetime.utcnow() - timedelta(days=7)

    subjects = (
        db.query(Email.subject)
        .filter(Email.received_at >= since, Email.subject.isnot(None))
        .all()
    )

    if not subjects:
        return "No subjects found in the last 7 days."

    total = len(subjects)
    questions = sum(1 for (s,) in subjects if "?" in s)
    with_emoji = sum(
        1 for (s,) in subjects
        if any(ord(c) > 0x1F600 for c in s)
    )
    with_numbers = sum(
        1 for (s,) in subjects
        if any(ch.isdigit() for ch in s)
    )
    urgency_words = {"limited", "hurry", "last chance", "ending", "urgent", "now", "today only", "final"}
    with_urgency = sum(
        1 for (s,) in subjects
        if any(w in s.lower() for w in urgency_words)
    )

    context = (
        f"Subject-line analysis from {total} emails this week:\n"
        f"- Questions: {questions} ({round(questions / total * 100)}%)\n"
        f"- With emoji: {with_emoji} ({round(with_emoji / total * 100)}%)\n"
        f"- With numbers: {with_numbers} ({round(with_numbers / total * 100)}%)\n"
        f"- Urgency words: {with_urgency} ({round(with_urgency / total * 100)}%)\n"
        "Generate a tweet sharing a quick, actionable insight from these stats."
    )
    return context


def _build_viral_thread(db: Session) -> str:
    """
    Query the ENTIRE database for surprising, counterintuitive patterns
    to feed into the viral thread prompt.
    """
    from sqlalchemy import text

    stats = {}

    # Overall stats
    row = db.execute(text("""
        SELECT COUNT(*) as total,
            ROUND(AVG(LENGTH(subject)), 1) as avg_len,
            COUNT(DISTINCT brand) as brand_count
        FROM emails WHERE subject IS NOT NULL
    """)).fetchone()
    stats["total_emails"] = row[0]
    stats["avg_subject_len"] = row[1]
    stats["brand_count"] = row[2]

    # Brands by volume (top 10)
    top_brands = db.execute(text("""
        SELECT brand, COUNT(*) as cnt, ROUND(AVG(LENGTH(subject)), 1) as avg_len
        FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL
        GROUP BY brand ORDER BY cnt DESC LIMIT 10
    """)).fetchall()
    stats["top_brands"] = [(r[0], r[1], r[2]) for r in top_brands]

    # Brands with ZERO exclamation marks (20+ emails)
    no_excl = db.execute(text("""
        SELECT brand, COUNT(*) as total
        FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL
        GROUP BY brand
        HAVING COUNT(*) >= 20
            AND SUM(CASE WHEN subject LIKE '%!%' THEN 1 ELSE 0 END) = 0
        ORDER BY total DESC
    """)).fetchall()
    stats["no_exclamation_brands"] = [(r[0], r[1]) for r in no_excl]

    # Brands with highest exclamation rate
    high_excl = db.execute(text("""
        SELECT brand, COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN subject LIKE '%!%' THEN 1 ELSE 0 END) / COUNT(*), 1) as pct
        FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL
        GROUP BY brand HAVING COUNT(*) >= 20
        ORDER BY pct DESC LIMIT 5
    """)).fetchall()
    stats["high_exclamation_brands"] = [(r[0], r[1], r[2]) for r in high_excl]

    # Shortest avg subject lines by brand
    short_subj = db.execute(text("""
        SELECT brand, ROUND(AVG(LENGTH(subject)), 1) as avg_len, COUNT(*) as cnt
        FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL
        GROUP BY brand HAVING COUNT(*) >= 20
        ORDER BY avg_len ASC LIMIT 5
    """)).fetchall()
    stats["shortest_subjects"] = [(r[0], r[1], r[2]) for r in short_subj]

    # Longest avg subject lines by brand
    long_subj = db.execute(text("""
        SELECT brand, ROUND(AVG(LENGTH(subject)), 1) as avg_len, COUNT(*) as cnt
        FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL
        GROUP BY brand HAVING COUNT(*) >= 20
        ORDER BY avg_len DESC LIMIT 5
    """)).fetchall()
    stats["longest_subjects"] = [(r[0], r[1], r[2]) for r in long_subj]

    # Discount word frequency
    disc = db.execute(text("""
        SELECT
            ROUND(100.0 * SUM(CASE WHEN LOWER(subject) LIKE '% off%' OR LOWER(subject) LIKE '%off %' THEN 1 ELSE 0 END) / COUNT(*), 1),
            ROUND(100.0 * SUM(CASE WHEN subject LIKE '%!%' THEN 1 ELSE 0 END) / COUNT(*), 1),
            ROUND(100.0 * SUM(CASE WHEN subject LIKE '%?%' THEN 1 ELSE 0 END) / COUNT(*), 1),
            ROUND(100.0 * SUM(CASE WHEN LOWER(subject) LIKE '%last chance%' OR LOWER(subject) LIKE '%ends today%' OR LOWER(subject) LIKE '%ends soon%' OR LOWER(subject) LIKE '%hurry%' THEN 1 ELSE 0 END) / COUNT(*), 1)
        FROM emails WHERE subject IS NOT NULL
    """)).fetchone()
    stats["pct_discount"] = disc[0]
    stats["pct_exclamation"] = disc[1]
    stats["pct_question"] = disc[2]
    stats["pct_urgency"] = disc[3]

    # Most interesting/creative short subject lines
    creative = db.execute(text("""
        SELECT brand, subject FROM emails
        WHERE subject IS NOT NULL AND LENGTH(subject) <= 20 AND LENGTH(subject) >= 3
        ORDER BY LENGTH(subject) ASC LIMIT 25
    """)).fetchall()
    stats["creative_short_subjects"] = [(r[0], r[1]) for r in creative]

    # ALL CAPS subject lines rate by brand
    caps_brands = db.execute(text("""
        SELECT brand, COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN subject = UPPER(subject) AND subject GLOB '*[A-Z]*' THEN 1 ELSE 0 END) / COUNT(*), 1) as caps_pct
        FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL
        GROUP BY brand HAVING COUNT(*) >= 20
        ORDER BY caps_pct DESC LIMIT 5
    """)).fetchall()
    stats["all_caps_brands"] = [(r[0], r[1], r[2]) for r in caps_brands]

    # Sample subject lines from top brands (for real examples)
    sample_subjects = {}
    for brand_name in ["Reformation", "Balenciaga", "Nykaa", "Net-A-Porter", "Nicobar", "Zomato"]:
        rows = db.execute(text(
            "SELECT subject FROM emails WHERE brand = :b AND subject IS NOT NULL ORDER BY RANDOM() LIMIT 10"
        ), {"b": brand_name}).fetchall()
        if rows:
            sample_subjects[brand_name] = [r[0] for r in rows]
    stats["sample_subjects"] = sample_subjects

    # Brands that use "Bestie" or casual language
    casual = db.execute(text("""
        SELECT brand, COUNT(*) as cnt FROM emails
        WHERE subject IS NOT NULL
            AND (LOWER(subject) LIKE '%bestie%' OR LOWER(subject) LIKE '%babe%' OR LOWER(subject) LIKE '%hey %')
        GROUP BY brand ORDER BY cnt DESC LIMIT 5
    """)).fetchall()
    stats["casual_language_brands"] = [(r[0], r[1]) for r in casual]

    # Build the context string
    lines = [
        f"=== MAILMUSE DATABASE STATS ===",
        f"Total emails analyzed: {stats['total_emails']}",
        f"Total brands: {stats['brand_count']}",
        f"Average subject line length: {stats['avg_subject_len']} characters",
        "",
        f"=== GLOBAL PATTERNS ===",
        f"Emails mentioning 'off' (discounts): {stats['pct_discount']}%",
        f"Emails with exclamation marks: {stats['pct_exclamation']}%",
        f"Emails with question marks: {stats['pct_question']}%",
        f"Emails with urgency language: {stats['pct_urgency']}%",
        "",
        f"=== TOP BRANDS BY EMAIL VOLUME ===",
    ]
    for brand, cnt, avg in stats["top_brands"]:
        lines.append(f"  {brand}: {cnt} emails, avg subject {avg} chars")

    lines.append("")
    lines.append("=== BRANDS WITH ZERO EXCLAMATION MARKS (20+ emails) ===")
    for brand, cnt in stats["no_exclamation_brands"]:
        lines.append(f"  {brand} ({cnt} emails)")

    lines.append("")
    lines.append("=== SHORTEST AVG SUBJECT LINES ===")
    for brand, avg, cnt in stats["shortest_subjects"]:
        lines.append(f"  {brand}: {avg} chars avg ({cnt} emails)")

    lines.append("")
    lines.append("=== LONGEST AVG SUBJECT LINES ===")
    for brand, avg, cnt in stats["longest_subjects"]:
        lines.append(f"  {brand}: {avg} chars avg ({cnt} emails)")

    lines.append("")
    lines.append("=== ALL CAPS USAGE BY BRAND ===")
    for brand, cnt, pct in stats["all_caps_brands"]:
        lines.append(f"  {brand}: {pct}% all caps ({cnt} emails)")

    lines.append("")
    lines.append("=== CREATIVE SHORT SUBJECT LINES ===")
    for brand, subj in stats["creative_short_subjects"]:
        lines.append(f"  {brand}: \"{subj}\"")

    lines.append("")
    lines.append("=== CASUAL LANGUAGE BRANDS ===")
    for brand, cnt in stats["casual_language_brands"]:
        lines.append(f"  {brand}: {cnt} emails with bestie/babe/hey")

    lines.append("")
    lines.append("=== SAMPLE SUBJECT LINES FROM KEY BRANDS ===")
    for brand, subjects in stats["sample_subjects"].items():
        lines.append(f"\n  {brand}:")
        for s in subjects:
            lines.append(f"    \"{s}\"")

    return "\n".join(lines)


# Mapping of tweet types to their context-builder functions
_TWEET_TYPE_BUILDERS = {
    "daily_digest": _build_daily_digest,
    "weekly_digest": _build_weekly_digest,
    "brand_spotlight": _build_brand_spotlight,
    "subject_line_insight": _build_subject_line_insight,
    "viral_thread": _build_viral_thread,
}


def generate_tweet_content(tweet_type: str, db: Optional[Session] = None) -> str:
    """
    Generate tweet text for a given *tweet_type* using live DB data and Claude.

    For ``viral_thread`` type, returns multiple tweets separated by ``\\n---\\n``.
    For all other types, returns a single tweet with the MailMuse URL appended.
    """
    if tweet_type not in _TWEET_TYPE_BUILDERS:
        raise ValueError(
            f"Unknown tweet_type '{tweet_type}'. "
            f"Valid types: {list(_TWEET_TYPE_BUILDERS.keys())}"
        )

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        # 1. Build data context from DB
        builder = _TWEET_TYPE_BUILDERS[tweet_type]
        context = builder(db)

        if tweet_type == "viral_thread":
            return _generate_viral_thread(context)

        # 2. Call Claude to craft a single tweet
        user_prompt = (
            f"Tweet type: {tweet_type}\n\n"
            f"Data context:\n{context}\n\n"
            f"Write a tweet (max {MAX_TWEET_BODY_LEN} characters). "
            "Do NOT include any URL — it will be appended automatically."
        )

        tweet_body = _call_claude_for_tweet(SYSTEM_PROMPT, user_prompt)

        # Ensure the body fits within the limit
        if len(tweet_body) > MAX_TWEET_BODY_LEN:
            tweet_body = tweet_body[: MAX_TWEET_BODY_LEN - 1] + "\u2026"

        # 3. Append the site URL
        tweet = tweet_body + SITE_URL
        return tweet

    finally:
        if close_db:
            db.close()


def _generate_viral_thread(context: str) -> str:
    """
    Generate a viral Twitter thread using Claude Sonnet and the full
    viral frameworks prompt. Returns tweets separated by \\n---\\n.
    """
    client = _get_anthropic_client()

    user_prompt = (
        "Using the database stats below, write a VIRAL Twitter thread "
        "(7-9 tweets). Pick the most surprising/counterintuitive data "
        "points and use the frameworks from your instructions.\n\n"
        f"{context}\n\n"
        "Remember: tweet 1 hook must be under 110 chars. Each tweet under "
        "280 chars. Separate tweets with --- on its own line. Last tweet "
        "should mention mailmuse.in as CTA."
    )

    response = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2500,
        system=VIRAL_THREAD_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
        timeout=60,
    )

    result = response.content[0].text.strip()

    # Strip markdown code fences if present
    if result.startswith("```"):
        result = result.split("```")[1]
        if result.startswith("text") or result.startswith("json"):
            result = result[4:]
        result = result.strip()

    return result
