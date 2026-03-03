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


def _call_claude_for_tweet(
    system_prompt: str,
    user_prompt: str,
    model: str = "claude-haiku-4-5-20251001",
    max_tokens: int = 200,
) -> str:
    """
    Ask Claude to write a tweet.  Returns the raw text from the model.
    """
    client = _get_anthropic_client()

    timeout = 60 if "sonnet" in model else 25
    response = client.messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system_prompt,
        messages=[
            {"role": "user", "content": user_prompt},
        ],
        timeout=timeout,
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


def _find_active_window(db: Session, preferred_days: int = 1) -> datetime:
    """
    Return a 'since' datetime that has enough data.
    Tries preferred window first, then widens to 7d, 30d, 90d, then all-time.
    """
    for days in [preferred_days, 7, 30, 90, 365, 3650]:
        since = datetime.utcnow() - timedelta(days=days)
        count = (
            db.query(func.count(Email.id))
            .filter(Email.received_at >= since)
            .scalar()
        ) or 0
        if count >= 10:
            return since
    # Fallback: all-time
    return datetime(2000, 1, 1)


def _window_label(since: datetime) -> str:
    """Human-readable label for the time window."""
    days = (datetime.utcnow() - since).days
    if days <= 1:
        return "today"
    elif days <= 7:
        return "this week"
    elif days <= 30:
        return "this month"
    elif days <= 90:
        return "in the last 3 months"
    else:
        return "across our archive"


def _build_daily_digest(db: Session) -> str:
    """Query recent emails and build a daily-digest tweet."""
    since = _find_active_window(db, preferred_days=1)
    window = _window_label(since)

    rows = (
        db.query(Email.brand, func.count(Email.id).label("cnt"))
        .filter(Email.received_at >= since, Email.brand.isnot(None))
        .group_by(Email.brand)
        .order_by(func.count(Email.id).desc())
        .limit(5)
        .all()
    )

    if not rows:
        return "No emails found in the database."

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
        f"Top email campaigns tracked on MailMuse {window}:\n"
        + "\n".join(lines)
    )
    return context


def _build_weekly_digest(db: Session) -> str:
    """Query recent data and build a weekly-digest style tweet."""
    since = _find_active_window(db, preferred_days=7)
    window = _window_label(since)

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

    # Pick a few interesting subjects for flavour
    interesting = (
        db.query(Email.brand, Email.subject)
        .filter(Email.received_at >= since, Email.subject.isnot(None))
        .order_by(func.random())
        .limit(5)
        .all()
    )
    subject_examples = "\n".join(f"  - {b}: \"{s[:60]}\"" for b, s in interesting) if interesting else ""

    context = (
        f"MailMuse {window}: {total_emails} campaigns tracked from "
        f"{total_brands} brands. Top brand: {top_brand}. "
        f"Trending campaign type: {trending_type}.\n"
        f"Sample subject lines:\n{subject_examples}"
    )
    return context


def _build_brand_spotlight(db: Session) -> str:
    """Pick a random brand with 3+ emails and spotlight it."""
    since = _find_active_window(db, preferred_days=7)

    active_brands = (
        db.query(Email.brand, func.count(Email.id).label("cnt"))
        .filter(Email.received_at >= since, Email.brand.isnot(None))
        .group_by(Email.brand)
        .having(func.count(Email.id) >= 3)
        .all()
    )

    if not active_brands:
        return (
            "No brand has 3 or more emails — "
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

    # Grab sample subjects for this brand
    samples = (
        db.query(Email.subject)
        .filter(Email.brand == brand, Email.subject.isnot(None))
        .order_by(func.random())
        .limit(8)
        .all()
    )
    sample_lines = "\n".join(f"  \"{s[0][:70]}\"" for s in samples) if samples else ""

    context = (
        f"Brand spotlight — {brand}:\n"
        f"- Total emails tracked: {email_count}\n"
        f"- Favourite send day: {fav_day}\n"
        f"- Avg subject-line length: {avg_subject_len} chars\n"
        f"- Sample subject lines:\n{sample_lines}\n"
        "Write an insightful tweet about this brand's email strategy."
    )
    return context


def _build_subject_line_insight(db: Session) -> str:
    """Analyse subject-line patterns from recent data."""
    since = _find_active_window(db, preferred_days=7)
    window = _window_label(since)

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

    # Pick a few example subjects for colour
    sample = random.sample([(s,) for (s,) in subjects], min(8, len(subjects)))
    examples = "\n".join(f"  \"{s[0][:70]}\"" for s in sample)

    context = (
        f"Subject-line analysis from {total} emails {window}:\n"
        f"- Questions: {questions} ({round(questions / total * 100)}%)\n"
        f"- With emoji: {with_emoji} ({round(with_emoji / total * 100)}%)\n"
        f"- With numbers: {with_numbers} ({round(with_numbers / total * 100)}%)\n"
        f"- Urgency words: {with_urgency} ({round(with_urgency / total * 100)}%)\n"
        f"Example subject lines:\n{examples}\n"
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
    # Use SIMILAR TO for Postgres, avoids SQLite-only GLOB
    caps_brands = db.execute(text("""
        SELECT brand, COUNT(*) as total,
            ROUND(100.0 * SUM(CASE WHEN subject = UPPER(subject) AND LENGTH(subject) > 3 THEN 1 ELSE 0 END) / COUNT(*), 1) as caps_pct
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


# ---------------------------------------------------------------------------
# NEW PROMPT SYSTEM: 10 Master Prompts for Twitter Content Engine
# ---------------------------------------------------------------------------

P1_EMAIL_TEARDOWN_SYSTEM = (
    "You are the voice of MailMuse — an authoritative email marketing intelligence "
    "platform. Your tone is sharp, data-informed, and confident. You never sound salesy. "
    "You sound like the smartest person in the email marketing room.\n\n"
    "TASK: Given the following brand email campaign data, write a single tweet (max 280 chars) "
    "that breaks down one clever tactic the brand used, frames it as an insight, and ends with "
    "a subtle hook back to MailMuse.\n\n"
    "RULES:\n"
    "1. Lead with the insight, not the brand name — make the reader think 'I need to know this'\n"
    "2. Use specific language: '3-email welcome sequence' not 'a few emails'\n"
    "3. Never use hashtags unless absolutely necessary (max 1)\n"
    "4. End with ONE of these CTA styles (rotate):\n"
    "   - 'We broke this down on MailMuse -> mailmuse.in'\n"
    "   - 'See the full teardown -> mailmuse.in'\n"
    "   - 'Tracked on MailMuse. Link in bio.'\n"
    "5. No emojis in the first line. Max 1 emoji in the full tweet.\n"
    "6. Sound like a strategist, not a fan\n\n"
    "Return ONLY the tweet text — no quotes, no labels, no extra formatting."
)

P2_STRATEGY_THREAD_SYSTEM = (
    "You are MailMuse's editorial voice — the authority on email marketing strategy. "
    "Write a Twitter/X thread (5-7 tweets) analyzing a brand's email marketing approach.\n\n"
    "THREAD STRUCTURE:\n"
    "Tweet 1 (Hook): A bold, curiosity-driven opener. Pattern: '[Brand] sends X emails per week. "
    "But here's what most people miss about their strategy 🧵'\n"
    "Tweet 2-3 (Breakdown): Specific tactics with concrete details. What they do differently. "
    "Reference actual email types and sequences.\n"
    "Tweet 4 (The 'Why It Works' tweet): Explain the strategic reasoning — connect tactics to "
    "email marketing principles.\n"
    "Tweet 5 (Counter-intuitive insight): Share something surprising from the data. "
    "Pattern: 'What surprised us: [unexpected finding]'\n"
    "Tweet 6 (Takeaway): One actionable lesson any marketer can steal. Be specific.\n"
    "Tweet 7 (CTA): Drive to MailMuse. Pattern: 'We track [Brand]'s full email strategy on "
    "MailMuse — every campaign, every sequence, updated live -> mailmuse.in'\n\n"
    "RULES:\n"
    "1. Each tweet must stand alone if someone only sees one\n"
    "2. Use 'we tracked' / 'we analyzed' / 'our data shows' — establish MailMuse as the source\n"
    "3. No generic advice like 'personalization matters' — be surgically specific\n"
    "4. Thread numbering: use '1/' '2/' etc.\n"
    "5. Max 1 emoji per tweet, zero is fine\n"
    "6. Never say 'game-changer' or 'revolutionary'\n"
    "7. Each tweet MUST be under 280 characters\n\n"
    "OUTPUT FORMAT:\n"
    "Return ONLY the thread tweets, separated by exactly '---' on its own line.\n"
    "No extra formatting. Just the raw tweet text between --- separators."
)

P3_BLOG_PROMO_SYSTEM = (
    "You are MailMuse's growth marketer writing tweets to drive clicks to blog posts and "
    "landing pages. Your voice is authoritative, slightly contrarian, and always specific.\n\n"
    "WRITE 3 TWEET VARIANTS (separate each with --- on its own line):\n\n"
    "Variant A — 'The Stat Hook': Lead with a surprising data point from the content. "
    "Pattern: '[Surprising stat]. We dug into why -> [link]'\n\n"
    "Variant B — 'The Contrarian': Challenge a common belief, then link to the proof. "
    "Pattern: 'Everyone says [common belief]. The data says otherwise -> [link]'\n\n"
    "Variant C — 'The Listicle Tease': Preview 1 of several insights to create curiosity. "
    "Pattern: 'We analyzed [X brands'] [email type]. Finding #3 will change how you [action] -> [link]'\n\n"
    "RULES:\n"
    "1. Every variant must be under 260 characters (leave room for the link)\n"
    "2. The link placeholder is [link] — system will replace with actual URL\n"
    "3. Never start with 'Check out our new blog post' or any variation\n"
    "4. Never use 'you won't believe' or clickbait language\n"
    "5. Address the reader as a peer, not a student\n"
    "6. Each variant should feel like it came from a different person on the same team\n\n"
    "Return ONLY the 3 variants separated by --- on its own line. No labels or formatting."
)

P4_SMART_REPLY_SYSTEM = (
    "You are replying to tweets about email marketing on behalf of MailMuse. Your goal is "
    "to add genuine value FIRST, then earn the right to mention MailMuse only when relevant.\n\n"
    "GENERATE A REPLY following these tiers:\n\n"
    "TIER 1 — VALUE REPLY (use 70% of the time):\n"
    "Add a specific insight, stat, or tactical tip related to what they said. Do NOT mention "
    "MailMuse. Just be genuinely helpful. Build reputation.\n\n"
    "TIER 2 — VALUE + SOFT MENTION (use 25% of the time):\n"
    "Add value, then naturally reference MailMuse as a source without being pushy.\n\n"
    "TIER 3 — DIRECT RECOMMENDATION (use 5% of the time, only when someone explicitly asks "
    "for tools/resources):\n"
    "Directly recommend MailMuse only when someone asks 'what tools do you use' or "
    "'where can I find email examples.'\n\n"
    "RULES:\n"
    "1. NEVER reply with just a link. Always lead with value.\n"
    "2. NEVER be sycophantic ('Great point!' 'So true!'). Get to the substance.\n"
    "3. Keep replies under 200 characters when possible — punchy wins on X\n"
    "4. Match the energy of the original tweet (casual -> casual, technical -> technical)\n"
    "5. If the original tweet is negative/ranting, empathize briefly, then offer a constructive angle\n"
    "6. Never argue or correct people publicly — reframe instead\n"
    "7. The reply should make the original author want to follow MailMuse\n\n"
    "Return ONLY the reply text — no quotes, no labels, no extra formatting."
)

P5_ENGAGEMENT_BAIT_SYSTEM = (
    "You are MailMuse's community voice. Write tweets designed to maximise replies and "
    "engagement from email marketers. These are NOT promotional — they're conversation "
    "starters that position MailMuse as the center of email marketing discourse.\n\n"
    "GENERATE 2 VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — 'The Hot Take Question':\n"
    "Ask a question that email marketers have strong opinions about. It should be specific "
    "enough that people MUST reply.\n"
    "Pattern: '[Specific scenario]. What would you do — A or B?'\n\n"
    "Variant B — 'The Show Your Work Prompt':\n"
    "Ask people to share their own data, strategies, or examples.\n"
    "Pattern: 'Drop your [specific metric/strategy]. I'll start: [MailMuse's own example]'\n\n"
    "RULES:\n"
    "1. Questions must be specific enough to answer in 1-2 sentences\n"
    "2. Avoid yes/no questions — force a choice or a share\n"
    "3. Never ask something Google could answer — ask for opinions and experience\n"
    "4. Include MailMuse's own answer when using 'Show Your Work' format\n"
    "5. No hashtags on engagement tweets — they reduce replies\n"
    "6. These tweets should NEVER contain a link\n"
    "7. Each variant must be under 280 characters\n\n"
    "Return ONLY the 2 variants separated by --- on its own line. No labels or formatting."
)

P6_BRAND_COMPARISON_SYSTEM = (
    "You are MailMuse's analyst. Write a tweet comparing two brands' email marketing approaches. "
    "The goal: make marketers curious enough to explore the full comparison on MailMuse.\n\n"
    "TWEET FORMAT:\n"
    "'[Brand A] vs [Brand B] — two [industry] giants, completely different email playbooks.\n\n"
    "[Brand A]: [1 specific tactic with data]\n"
    "[Brand B]: [1 contrasting tactic with data]\n\n"
    "Who's doing it better? We broke it down -> mailmuse.in'\n\n"
    "RULES:\n"
    "1. Never declare a winner — let the reader decide (this drives clicks AND replies)\n"
    "2. Use concrete numbers, not vague descriptors\n"
    "3. The comparison must highlight a genuinely interesting strategic difference\n"
    "4. Keep the entire tweet under 280 characters\n"
    "5. Format the brand names in a way that fans of both brands will notice in their timeline\n\n"
    "Return ONLY the tweet text — no quotes, no labels, no extra formatting."
)

P7_WEEKLY_ROUNDUP_SYSTEM = (
    "You are MailMuse's data team publishing a weekly insight. Write a tweet summarizing one "
    "notable email marketing trend observed across the brands MailMuse tracks.\n\n"
    "TWEET FORMAT:\n"
    "📊 MailMuse Weekly: [Trend headline in 10 words or fewer]\n\n"
    "[1-2 sentences explaining the trend with specifics]\n\n"
    "Brands doing this: [Brand 1], [Brand 2], [Brand 3]\n\n"
    "Full data on mailmuse.in\n\n"
    "RULES:\n"
    "1. The trend must be specific and data-backed, not a vague observation\n"
    "2. Use the chart emoji ONLY for weekly roundup tweets (brand consistency)\n"
    "3. Name real brands — specificity drives engagement\n"
    "4. Keep the 'Full data' CTA simple — don't oversell\n"
    "5. Keep the entire tweet under 280 characters\n\n"
    "Return ONLY the tweet text — no quotes, no labels, no extra formatting."
)

P8_NEWSJACKING_SYSTEM = (
    "You are MailMuse's editorial voice reacting to breaking or trending email marketing news. "
    "Your take must add something the news article didn't — MailMuse's unique data angle.\n\n"
    "GENERATE 2 VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — 'The Data Reaction':\n"
    "Acknowledge the news, then add MailMuse's data perspective.\n"
    "Pattern: '[News in 1 sentence]. We looked at the data — here's what we're actually "
    "seeing: [MailMuse insight]'\n\n"
    "Variant B — 'The Prediction':\n"
    "Use the news as a launchpad to predict what happens next.\n"
    "Pattern: '[News]. Our take: this means [prediction]. Here's why -> [brief reasoning]. "
    "We'll be tracking the fallout on mailmuse.in'\n\n"
    "RULES:\n"
    "1. Never just summarize the news — add a unique angle\n"
    "2. Credit the source if referencing specific reporting\n"
    "3. Keep the hot take defensible — don't chase controversy for its own sake\n"
    "4. If MailMuse has no relevant data angle, use Variant B (prediction) instead\n"
    "5. Each variant must be under 280 characters\n\n"
    "Return ONLY the 2 variants separated by --- on its own line. No labels or formatting."
)

P9_SUBJECT_SPOTLIGHT_SYSTEM = (
    "You are MailMuse's subject line curator. Write a tweet highlighting a standout subject "
    "line spotted in the wild. This is a repeatable series format.\n\n"
    "TWEET FORMAT:\n"
    "Subject line of the day 👀\n\n"
    "Brand: [brand]\n"
    "Email: [type]\n"
    "Subject: \"[subject line]\"\n\n"
    "Why it works: [1-2 sentence analysis of the technique used]\n\n"
    "More subject lines tracked daily on mailmuse.in\n\n"
    "RULES:\n"
    "1. The analysis must be tactical — explain the psychological or strategic principle at play\n"
    "2. Rotate between different 'why it works' angles: curiosity, specificity, urgency, "
    "social proof, personalization, pattern interrupt\n"
    "3. Never just say 'it's catchy' — explain WHY it catches attention\n"
    "4. Use the eyes emoji ONLY for this series (brand consistency)\n"
    "5. Keep the subject line in quotes exactly as it appeared\n"
    "6. Keep the entire tweet under 280 characters\n\n"
    "Return ONLY the tweet text — no quotes, no labels, no extra formatting."
)

P10_FOLLOW_UP_REPLY_SYSTEM = (
    "You are MailMuse continuing a conversation on X/Twitter. Someone engaged with your "
    "previous reply. Write a follow-up that deepens the conversation and — if appropriate — "
    "drives them to MailMuse.\n\n"
    "FOLLOW-UP STRATEGY:\n\n"
    "If they AGREED with you:\n"
    "-> Add a deeper layer. 'Yeah — and what's even more interesting is [deeper insight]. "
    "We actually track this across [X] brands if you want to dig in -> [link]'\n\n"
    "If they DISAGREED or pushed back:\n"
    "-> Acknowledge their point, offer nuance, never argue. 'Fair point — it probably depends "
    "on [variable]. We've seen it go both ways. Here's some data that might add context -> [link]'\n\n"
    "If they ASKED A QUESTION:\n"
    "-> Answer it directly and thoroughly. If MailMuse has relevant data, link to it naturally.\n\n"
    "If they SHARED THEIR OWN EXPERIENCE:\n"
    "-> Validate, connect to broader patterns. 'That's a great example — and you're not alone. "
    "We've seen [X]% of [industry] brands doing something similar.'\n\n"
    "RULES:\n"
    "1. Only include a link if it genuinely adds value to the conversation\n"
    "2. Never repeat your original point — always go deeper\n"
    "3. Keep follow-ups shorter than original replies — be more casual\n"
    "4. If the conversation is flowing well without links, don't force one in\n"
    "5. Match the conversational energy — if they're casual, be casual\n"
    "6. Aim to get them to follow MailMuse, not just click once\n\n"
    "Return ONLY the reply text — no quotes, no labels, no extra formatting."
)


# ---------------------------------------------------------------------------
# Tactic detection helper
# ---------------------------------------------------------------------------

def _detect_tactic(subject: str) -> str:
    """Detect the primary email marketing tactic from a subject line."""
    lower = subject.lower()
    if any(w in lower for w in ["last chance", "ending", "hurry", "urgent", "final", "limited"]):
        return "urgency/scarcity"
    if any(w in lower for w in ["just for you", "your ", "personalized", "picked for", "recommended"]):
        return "personalization"
    if any(w in lower for w in ["best seller", "trending", "popular", "everyone", "top rated"]):
        return "social proof"
    if any(w in lower for w in ["% off", "sale", "discount", "save ", "deal"]):
        return "discount/promotion"
    if any(w in lower for w in ["new", "introducing", "just dropped", "just launched", "first look"]):
        return "novelty/exclusivity"
    if any(w in lower for w in ["back in stock", "restocked", "available again"]):
        return "scarcity (restock)"
    if "?" in subject:
        return "curiosity/question"
    if any(w in lower for w in ["free", "gift", "bonus", "complimentary"]):
        return "incentive/freebie"
    return "brand storytelling"


# ---------------------------------------------------------------------------
# Data builders for new tweet types (P1–P10)
# ---------------------------------------------------------------------------

def _build_email_teardown(db: Session, **kwargs) -> str:
    """P1: Pick a random brand email and build teardown context."""
    since = _find_active_window(db, preferred_days=7)

    active_brands = (
        db.query(Email.brand, func.count(Email.id).label("cnt"))
        .filter(Email.received_at >= since, Email.brand.isnot(None))
        .group_by(Email.brand)
        .having(func.count(Email.id) >= 3)
        .all()
    )
    if not active_brands:
        raise ValueError("Not enough email data for teardown")

    brand, count = random.choice(active_brands)

    email = (
        db.query(Email)
        .filter(Email.brand == brand, Email.received_at >= since)
        .order_by(Email.received_at.desc())
        .first()
    )
    email_type = email.type or "Promotional"
    subject = email.subject
    tactic = _detect_tactic(subject)

    freq_days = max((datetime.utcnow() - since).days, 1)
    frequency = f"{round(count / freq_days * 7, 1)} emails/week"

    return (
        f"Brand name: {brand}\n"
        f"Email type: {email_type}\n"
        f"Subject line: {subject}\n"
        f"Key tactic observed: {tactic}\n"
        f"Send frequency: {frequency}"
    )


def _build_strategy_thread(db: Session, **kwargs) -> str:
    """P2: Build brand strategy data for a deep-dive thread."""
    brand_name = kwargs.get("brand_name")
    since = _find_active_window(db, preferred_days=30)

    if brand_name:
        count = (
            db.query(func.count(Email.id))
            .filter(Email.brand == brand_name, Email.received_at >= since)
            .scalar()
        ) or 0
        if count < 5:
            raise ValueError(f"Not enough data for {brand_name}")
        brand = brand_name
    else:
        brands = (
            db.query(Email.brand, func.count(Email.id).label("cnt"))
            .filter(Email.received_at >= since, Email.brand.isnot(None))
            .group_by(Email.brand)
            .having(func.count(Email.id) >= 10)
            .order_by(func.count(Email.id).desc())
            .limit(20)
            .all()
        )
        if not brands:
            raise ValueError("Not enough data for strategy thread")
        brand, _ = random.choice(brands)

    types = (
        db.query(Email.type, func.count(Email.id).label("cnt"))
        .filter(Email.brand == brand, Email.received_at >= since, Email.type.isnot(None))
        .group_by(Email.type)
        .order_by(func.count(Email.id).desc())
        .all()
    )
    email_types_list = ", ".join(f"{t} ({c})" for t, c in types)

    industry_row = (
        db.query(Email.industry)
        .filter(Email.brand == brand, Email.industry.isnot(None))
        .first()
    )
    industry = industry_row[0] if industry_row else "DTC/Ecommerce"

    total = (
        db.query(func.count(Email.id))
        .filter(Email.brand == brand, Email.received_at >= since)
        .scalar()
    ) or 0
    freq_days = max((datetime.utcnow() - since).days, 1)
    emails_per_week = round(total / freq_days * 7, 1)

    subjects = (
        db.query(Email.subject)
        .filter(Email.brand == brand, Email.subject.isnot(None))
        .all()
    )
    avg_subj_len = round(sum(len(s[0]) for s in subjects) / len(subjects)) if subjects else 0

    patterns = f"Send frequency: ~{emails_per_week} emails/week. Avg subject length: {avg_subj_len} chars."
    top_type = types[0][0] if types else "Newsletter"
    data_points = f"Avg emails/week: {emails_per_week}. Top-performing type: {top_type}. Total tracked: {total}."

    return (
        f"Brand name: {brand}\n"
        f"Industry/vertical: {industry}\n"
        f"Email types tracked: {email_types_list}\n"
        f"Notable patterns: {patterns}\n"
        f"Data points: {data_points}"
    )


def _build_blog_promo(db: Session, **kwargs) -> str:
    """P3: Build context for blog/landing page promo (needs params)."""
    url = kwargs.get("url", "")
    title = kwargs.get("title", "")
    topic = kwargs.get("topic", "")
    key_stat = kwargs.get("key_stat", "")
    audience = kwargs.get("audience", "ecommerce marketers")

    if not url or not title:
        raise ValueError("blog_promo requires 'url' and 'title' parameters")

    return (
        f"Page URL: {url}\n"
        f"Page title: {title}\n"
        f"Core topic: {topic}\n"
        f"Key stat or finding: {key_stat}\n"
        f"Target audience: {audience}"
    )


def _build_smart_reply(db: Session, **kwargs) -> str:
    """P4: Build context for replying to an email marketing tweet."""
    tweet_text = kwargs.get("tweet_text", "")
    author_handle = kwargs.get("author_handle", "")
    category = kwargs.get("category", "email marketing")

    if not tweet_text:
        raise ValueError("smart_reply requires 'tweet_text' parameter")

    data_point = kwargs.get("data_point", "")
    if not data_point:
        since = _find_active_window(db, preferred_days=7)
        total = (
            db.query(func.count(Email.id))
            .filter(Email.received_at >= since)
            .scalar()
        ) or 0
        brands = (
            db.query(func.count(func.distinct(Email.brand)))
            .filter(Email.received_at >= since, Email.brand.isnot(None))
            .scalar()
        ) or 0
        data_point = f"MailMuse tracks {brands}+ brands and {total}+ email campaigns"

    return (
        f"Original tweet text: {tweet_text}\n"
        f"Tweet author handle: {author_handle}\n"
        f"Tweet topic category: {category}\n"
        f"Relevant MailMuse data point: {data_point}"
    )


def _build_engagement_bait(db: Session, **kwargs) -> str:
    """P5: Build context for engagement/question tweets."""
    topic = kwargs.get("topic", "")
    trending = kwargs.get("trending", "")

    if not topic:
        topics = [
            "subject lines", "send timing", "automation", "email design",
            "deliverability", "welcome sequences", "abandoned cart emails",
            "promotional emails", "segmentation", "open rates",
        ]
        topic = random.choice(topics)

    return (
        f"Topic area: {topic}\n"
        f"Trending angle (optional): {trending}"
    )


def _build_brand_comparison(db: Session, **kwargs) -> str:
    """P6: Build comparison data for two brands."""
    brand_a = kwargs.get("brand_a", "")
    brand_b = kwargs.get("brand_b", "")
    since = _find_active_window(db, preferred_days=30)

    if not brand_a or not brand_b:
        brands = (
            db.query(Email.brand, func.count(Email.id).label("cnt"))
            .filter(Email.received_at >= since, Email.brand.isnot(None))
            .group_by(Email.brand)
            .having(func.count(Email.id) >= 10)
            .order_by(func.count(Email.id).desc())
            .limit(20)
            .all()
        )
        if len(brands) < 2:
            raise ValueError("Not enough brands for comparison")
        selected = random.sample(brands, 2)
        brand_a = selected[0][0]
        brand_b = selected[1][0]

    def _brand_stats(brand):
        total = (
            db.query(func.count(Email.id))
            .filter(Email.brand == brand, Email.received_at >= since)
            .scalar()
        ) or 0
        freq_days = max((datetime.utcnow() - since).days, 1)
        per_week = round(total / freq_days * 7, 1)
        subjects = (
            db.query(Email.subject)
            .filter(Email.brand == brand, Email.subject.isnot(None))
            .limit(50)
            .all()
        )
        avg_len = round(sum(len(s[0]) for s in subjects) / len(subjects)) if subjects else 0
        top_type_row = (
            db.query(Email.type, func.count(Email.id))
            .filter(Email.brand == brand, Email.type.isnot(None))
            .group_by(Email.type)
            .order_by(func.count(Email.id).desc())
            .first()
        )
        top = top_type_row[0] if top_type_row else "Unknown"
        ind_row = (
            db.query(Email.industry)
            .filter(Email.brand == brand, Email.industry.isnot(None))
            .first()
        )
        ind = ind_row[0] if ind_row else "DTC/Ecommerce"
        return {"total": total, "per_week": per_week, "avg_subject_len": avg_len, "top_type": top, "industry": ind}

    stats_a = _brand_stats(brand_a)
    stats_b = _brand_stats(brand_b)

    differences = []
    if abs(stats_a["per_week"] - stats_b["per_week"]) > 1:
        differences.append(f"send frequency ({stats_a['per_week']}/week vs {stats_b['per_week']}/week)")
    if abs(stats_a["avg_subject_len"] - stats_b["avg_subject_len"]) > 10:
        differences.append(f"subject line length ({stats_a['avg_subject_len']} chars vs {stats_b['avg_subject_len']} chars)")
    if stats_a["top_type"] != stats_b["top_type"]:
        differences.append(f"email types ({stats_a['top_type']} vs {stats_b['top_type']})")
    difference = differences[0] if differences else "overall email strategy approach"

    data_a = f"{stats_a['per_week']} emails/week, avg subject {stats_a['avg_subject_len']} chars, top type: {stats_a['top_type']}"
    data_b = f"{stats_b['per_week']} emails/week, avg subject {stats_b['avg_subject_len']} chars, top type: {stats_b['top_type']}"

    return (
        f"Brand A: {brand_a}\n"
        f"Brand B: {brand_b}\n"
        f"Industry: {stats_a['industry']}\n"
        f"Key difference: {difference}\n"
        f"Data points — {brand_a}: {data_a}\n"
        f"Data points — {brand_b}: {data_b}"
    )


def _build_weekly_roundup(db: Session, **kwargs) -> str:
    """P7: Build weekly trend data from this week vs last week."""
    now = datetime.utcnow()
    this_week = now - timedelta(days=7)
    last_week = this_week - timedelta(days=7)

    def _week_stats(since, until):
        total = (
            db.query(func.count(Email.id))
            .filter(Email.received_at >= since, Email.received_at < until)
            .scalar()
        ) or 0
        types = (
            db.query(Email.type, func.count(Email.id).label("cnt"))
            .filter(Email.received_at >= since, Email.received_at < until, Email.type.isnot(None))
            .group_by(Email.type)
            .order_by(func.count(Email.id).desc())
            .all()
        )
        return {"total": total, "types": types}

    current = _week_stats(this_week, now)
    previous = _week_stats(last_week, this_week)

    current_types = {t: c for t, c in current["types"]}
    previous_types = {t: c for t, c in previous["types"]}

    trends = []
    for t, c in current_types.items():
        prev_c = previous_types.get(t, 0)
        if prev_c > 0:
            change = round((c - prev_c) / prev_c * 100)
            if abs(change) >= 10:
                trends.append((t, change, c))

    if trends:
        trends.sort(key=lambda x: abs(x[1]), reverse=True)
        trend_type, trend_change, _ = trends[0]
        direction = "more" if trend_change > 0 else "fewer"
        trend = f"{abs(trend_change)}% {direction} {trend_type} emails this week vs last week"
    else:
        trend = f"{current['total']} email campaigns tracked this week across {len(current_types)} categories"

    if trends:
        examples_rows = (
            db.query(Email.brand)
            .filter(Email.received_at >= this_week, Email.type == trends[0][0], Email.brand.isnot(None))
            .group_by(Email.brand)
            .order_by(func.count(Email.id).desc())
            .limit(3)
            .all()
        )
    else:
        examples_rows = (
            db.query(Email.brand)
            .filter(Email.received_at >= this_week, Email.brand.isnot(None))
            .group_by(Email.brand)
            .order_by(func.count(Email.id).desc())
            .limit(3)
            .all()
        )
    examples = ", ".join(r[0] for r in examples_rows) if examples_rows else "N/A"

    week_str = now.strftime("%b %d, %Y")
    explanation = kwargs.get("explanation", "Seasonal campaigns and shifting engagement patterns likely driving this.")

    return (
        f"Week/date: Week of {week_str}\n"
        f"Trend observed: {trend}\n"
        f"Supporting examples: {examples}\n"
        f"Possible explanation: {explanation}"
    )


def _build_newsjacking(db: Session, **kwargs) -> str:
    """P8: Build context for reacting to email marketing news (needs params)."""
    headline = kwargs.get("headline", "")
    summary = kwargs.get("summary", "")
    source = kwargs.get("source", "")

    if not headline:
        raise ValueError("newsjacking requires 'headline' parameter")

    mailmuse_angle = kwargs.get("mailmuse_angle", "")
    if not mailmuse_angle:
        since = _find_active_window(db, preferred_days=7)
        total = (
            db.query(func.count(Email.id))
            .filter(Email.received_at >= since)
            .scalar()
        ) or 0
        brands = (
            db.query(func.count(func.distinct(Email.brand)))
            .filter(Email.received_at >= since, Email.brand.isnot(None))
            .scalar()
        ) or 0
        mailmuse_angle = (
            f"We track {brands}+ brands and {total}+ campaigns — "
            "watching how brands respond in real-time."
        )

    return (
        f"News headline: {headline}\n"
        f"News summary: {summary}\n"
        f"MailMuse's relevant data angle: {mailmuse_angle}\n"
        f"Source: {source}"
    )


def _build_subject_spotlight(db: Session, **kwargs) -> str:
    """P9: Pick a standout subject line and build spotlight context."""
    since = _find_active_window(db, preferred_days=7)

    candidates = []

    # Short creative subjects
    short = (
        db.query(Email)
        .filter(
            Email.received_at >= since,
            Email.subject.isnot(None),
            func.length(Email.subject) <= 30,
            func.length(Email.subject) >= 5,
        )
        .order_by(func.random())
        .limit(10)
        .all()
    )
    candidates.extend(short)

    # Question subjects
    questions = (
        db.query(Email)
        .filter(Email.received_at >= since, Email.subject.like("%?%"))
        .order_by(func.random())
        .limit(10)
        .all()
    )
    candidates.extend(questions)

    # Urgency subjects
    urgency_all = (
        db.query(Email)
        .filter(Email.received_at >= since, Email.subject.isnot(None))
        .order_by(func.random())
        .limit(30)
        .all()
    )
    candidates.extend(
        e for e in urgency_all
        if any(w in (e.subject or "").lower() for w in ["last chance", "ending", "hurry", "final hours"])
    )

    if not candidates:
        candidates = (
            db.query(Email)
            .filter(Email.received_at >= since, Email.subject.isnot(None))
            .order_by(func.random())
            .limit(5)
            .all()
        )

    if not candidates:
        raise ValueError("No emails found for subject spotlight")

    email = random.choice(candidates)
    brand = email.brand or "Unknown"
    subject = email.subject
    email_type = email.type or "Promotional"
    analysis = _detect_tactic(subject)

    return (
        f"Brand: {brand}\n"
        f"Subject line: {subject}\n"
        f"Email type: {email_type}\n"
        f"Why it works: {analysis}"
    )


def _build_follow_up_reply(db: Session, **kwargs) -> str:
    """P10: Build context for a follow-up reply (needs params)."""
    original_reply = kwargs.get("original_reply", "")
    their_response = kwargs.get("their_response", "")
    topic = kwargs.get("topic", "email marketing")
    relevant_link = kwargs.get("relevant_link", "mailmuse.in")

    if not original_reply or not their_response:
        raise ValueError("follow_up_reply requires 'original_reply' and 'their_response' parameters")

    return (
        f"Your original reply: {original_reply}\n"
        f"Their response to you: {their_response}\n"
        f"Conversation topic: {topic}\n"
        f"Relevant MailMuse page: {relevant_link}"
    )


# ---------------------------------------------------------------------------
# Reply Hub — 6 specialized reply styles (P11–P16)
# ---------------------------------------------------------------------------

P11_REPLY_DATA_DROP_SYSTEM = (
    "You are replying to a tweet on behalf of MailMuse, an email marketing intelligence "
    "platform that tracks 600+ D2C/ecommerce brand emails. Your account has <10 followers "
    "so this reply MUST be so insightful that people click your profile.\n\n"
    "STRATEGY: DATA DROP REPLY\n"
    "Drop a specific, surprising data point from MailMuse's database that directly relates "
    "to what the original tweet is discussing. The data should make the reader think "
    "'Wait, where did this person get that data?'\n\n"
    "GENERATE 3 REPLY VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — Lead with the most surprising stat\n"
    "Variant B — Frame the stat as a contrast to their point\n"
    "Variant C — Use the stat to add a layer they missed\n\n"
    "RULES:\n"
    "1. NEVER start with 'Great point!' or any sycophancy. Jump straight to the data.\n"
    "2. Every reply MUST contain a specific number or percentage from the provided data.\n"
    "3. Keep each variant under 220 characters — short replies get more reads.\n"
    "4. Mention 'we tracked/analyzed' to imply you have access to special data, "
    "but do NOT say MailMuse by name unless Variant C.\n"
    "5. The data point must feel like insider knowledge, not public information.\n"
    "6. If the original tweet is about a specific tactic, counter with real numbers.\n"
    "7. Format: [Data point]. [One sentence insight or implication].\n\n"
    "Return ONLY the 3 variants separated by --- on its own line. No labels or formatting."
)

P12_REPLY_CONTRARIAN_SYSTEM = (
    "You are replying to a tweet on behalf of MailMuse, an email marketing intelligence "
    "platform tracking 600+ D2C/ecommerce brand emails. Your account has <10 followers "
    "so this reply MUST be provocative enough to earn attention.\n\n"
    "STRATEGY: RESPECTFUL CONTRARIAN REPLY\n"
    "Challenge the original tweet's assumption with data. Not hostile — intellectually "
    "stimulating. Make people think 'Hmm, interesting counterpoint.'\n\n"
    "GENERATE 3 REPLY VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — 'Interesting, but our data across 600+ brands shows...'\n"
    "Variant B — 'This is true for [segment], but [other segment] tells a different story...'\n"
    "Variant C — 'Depends on the industry. In [industry], we see [opposite pattern]...'\n\n"
    "RULES:\n"
    "1. ALWAYS acknowledge their point before challenging — 'Interesting take' or "
    "'This tracks for [X], but...' Never be dismissive.\n"
    "2. The counter-argument MUST be backed by a specific data point from the provided context.\n"
    "3. End with curiosity, not a mic drop — invite discussion.\n"
    "4. Keep each variant under 240 characters.\n"
    "5. Never mention MailMuse by name — let the profile do the selling.\n"
    "6. The best contrarian replies add nuance, not disagreement.\n\n"
    "Return ONLY the 3 variants separated by --- on its own line. No labels or formatting."
)

P13_REPLY_EXAMPLE_SYSTEM = (
    "You are replying to a tweet on behalf of MailMuse, an email marketing intelligence "
    "platform tracking 600+ D2C/ecommerce brand emails. Your account has <10 followers.\n\n"
    "STRATEGY: REAL EXAMPLE SHOWCASE REPLY\n"
    "Share a concrete, real brand email example that illustrates or expands on the original "
    "tweet's point. People love specific examples they can learn from.\n\n"
    "GENERATE 3 REPLY VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — Share the brand + subject line as a 'perfect example'\n"
    "Variant B — Share the brand + tactic as a 'here's who does this well'\n"
    "Variant C — Share two contrasting examples from different brands\n\n"
    "RULES:\n"
    "1. Use the REAL brand name and REAL subject line from the provided context. "
    "Do NOT make up examples.\n"
    "2. Format the subject line in quotes: '[Brand] just sent: \"[subject line]\"'\n"
    "3. Add a one-line analysis of WHY this example is relevant to their point.\n"
    "4. Keep each variant under 250 characters.\n"
    "5. Do not mention MailMuse by name.\n"
    "6. The example must genuinely relate to the topic of the original tweet.\n\n"
    "Return ONLY the 3 variants separated by --- on its own line. No labels or formatting."
)

P14_REPLY_QUICK_TIP_SYSTEM = (
    "You are replying to a tweet on behalf of MailMuse. Your account has <10 followers "
    "so this reply must add so much value people want to follow you.\n\n"
    "STRATEGY: QUICK TACTICAL TIP REPLY\n"
    "Add a specific, actionable tip that builds on what they said. The kind of tip that "
    "makes someone screenshot the reply.\n\n"
    "GENERATE 3 REPLY VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — A specific 'how-to' that extends their point\n"
    "Variant B — A 'pro tip' with a concrete tactic\n"
    "Variant C — A '1 thing most people miss' angle\n\n"
    "RULES:\n"
    "1. The tip must be SPECIFIC. Not 'personalize your emails' but 'use the product "
    "they last browsed in your subject line — brands doing this see 2x open rates'.\n"
    "2. Ground tips in data from the provided context where possible.\n"
    "3. Keep each variant under 220 characters — punchy tips win.\n"
    "4. Never start with 'Great point!' — start with the tip itself.\n"
    "5. Do not mention MailMuse. Pure value.\n"
    "6. Format: [Tip]. [Why it works / data point].\n\n"
    "Return ONLY the 3 variants separated by --- on its own line. No labels or formatting."
)

P15_REPLY_AGREE_AMPLIFY_SYSTEM = (
    "You are replying to a tweet on behalf of MailMuse, tracking 600+ D2C brand emails. "
    "Your account has <10 followers.\n\n"
    "STRATEGY: AGREE + AMPLIFY REPLY\n"
    "Agree with their point and add a DEEPER layer they didn't mention. Make the original "
    "author feel validated while positioning yourself as someone who knows even more.\n\n"
    "GENERATE 3 REPLY VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — 'This. And it goes even deeper...' + data layer\n"
    "Variant B — 'Exactly. The brands doing this best also...' + pattern\n"
    "Variant C — 'Underrated point. Here's why this matters more than people think...' + insight\n\n"
    "RULES:\n"
    "1. Agreement must be brief (2-3 words max). The amplification is the star.\n"
    "2. The deeper layer MUST include a specific data point or brand example.\n"
    "3. Keep each variant under 240 characters.\n"
    "4. Never just agree — always add something the original author will learn from.\n"
    "5. Do not mention MailMuse unless Variant C and only naturally.\n"
    "6. The best agree+amplify makes the original author want to retweet your reply.\n\n"
    "Return ONLY the 3 variants separated by --- on its own line. No labels or formatting."
)

P16_REPLY_RESOURCE_DROP_SYSTEM = (
    "You are replying to a tweet on behalf of MailMuse, an email marketing intelligence "
    "platform tracking 600+ D2C/ecommerce brand emails. Your account has <10 followers.\n\n"
    "STRATEGY: RESOURCE DROP REPLY\n"
    "ONLY use this when someone explicitly asks for tools, resources, or email examples. "
    "Mention MailMuse naturally as one helpful resource.\n\n"
    "GENERATE 3 REPLY VARIANTS (separate with --- on its own line):\n\n"
    "Variant A — Answer their question first, mention MailMuse as 'one tool I use'\n"
    "Variant B — Give a useful tip first, then 'btw mailmuse.in has [specific thing]'\n"
    "Variant C — Share a specific finding, then 'we built this at mailmuse.in'\n\n"
    "RULES:\n"
    "1. This reply style should ONLY be used when someone asks for resources/tools.\n"
    "2. ALWAYS lead with value. The MailMuse mention should feel like a natural addition.\n"
    "3. Include the URL: mailmuse.in\n"
    "4. Keep each variant under 250 characters.\n"
    "5. Never sound like an ad. Sound like a peer sharing a tool they genuinely use.\n"
    "6. Reference a specific MailMuse capability (track emails, browse subject lines, "
    "compare brands) rather than generic 'check out MailMuse'.\n\n"
    "Return ONLY the 3 variants separated by --- on its own line. No labels or formatting."
)


# ---------------------------------------------------------------------------
# Reply Hub — data builder functions
# ---------------------------------------------------------------------------

def _build_reply_data_drop(db: Session, **kwargs) -> str:
    """Build data-rich context for a data drop reply."""
    tweet_text = kwargs.get("tweet_text", "")
    author_handle = kwargs.get("author_handle", "")
    target_category = kwargs.get("target_category", "")
    if not tweet_text:
        raise ValueError("reply_data_drop requires 'tweet_text' parameter")

    since = _find_active_window(db, preferred_days=30)

    total_emails = db.query(func.count(Email.id)).filter(Email.received_at >= since).scalar() or 0
    total_brands = db.query(func.count(func.distinct(Email.brand))).filter(
        Email.received_at >= since, Email.brand.isnot(None)
    ).scalar() or 0

    subjects = db.query(Email.subject).filter(
        Email.received_at >= since, Email.subject.isnot(None)
    ).all()
    total_subj = len(subjects)
    pct_questions = round(sum(1 for (s,) in subjects if "?" in s) / max(total_subj, 1) * 100, 1)
    pct_exclamation = round(sum(1 for (s,) in subjects if "!" in s) / max(total_subj, 1) * 100, 1)
    pct_urgency = round(sum(1 for (s,) in subjects if any(
        w in s.lower() for w in ["last chance", "hurry", "ending", "limited", "final"]
    )) / max(total_subj, 1) * 100, 1)
    avg_subject_len = round(sum(len(s) for (s,) in subjects) / max(total_subj, 1), 1)

    type_dist = db.query(Email.type, func.count(Email.id)).filter(
        Email.received_at >= since, Email.type.isnot(None)
    ).group_by(Email.type).order_by(func.count(Email.id).desc()).limit(5).all()
    type_lines = ", ".join(f"{t}: {c}" for t, c in type_dist)

    top_brands = db.query(Email.brand, func.count(Email.id)).filter(
        Email.received_at >= since, Email.brand.isnot(None)
    ).group_by(Email.brand).order_by(func.count(Email.id).desc()).limit(5).all()
    top_brand_lines = ", ".join(f"{b} ({c} emails)" for b, c in top_brands)

    return (
        f"Original tweet: {tweet_text}\n"
        f"Author: @{author_handle}\n"
        f"Author category: {target_category}\n\n"
        f"=== MAILMUSE DATA POINTS (pick the most relevant) ===\n"
        f"Total emails analyzed: {total_emails}\n"
        f"Total brands tracked: {total_brands}\n"
        f"Avg subject line length: {avg_subject_len} chars\n"
        f"Subject lines with questions: {pct_questions}%\n"
        f"Subject lines with exclamation marks: {pct_exclamation}%\n"
        f"Subject lines with urgency language: {pct_urgency}%\n"
        f"Top email types: {type_lines}\n"
        f"Most active brands: {top_brand_lines}\n"
    )


def _build_reply_contrarian(db: Session, **kwargs) -> str:
    """Build contrarian data context for challenging a tweet's premise."""
    tweet_text = kwargs.get("tweet_text", "")
    author_handle = kwargs.get("author_handle", "")
    if not tweet_text:
        raise ValueError("reply_contrarian requires 'tweet_text' parameter")

    since = _find_active_window(db, preferred_days=30)

    total_brands = db.query(func.count(func.distinct(Email.brand))).filter(
        Email.received_at >= since, Email.brand.isnot(None)
    ).scalar() or 0

    # Brands with shortest subject lines
    from sqlalchemy import text as sa_text
    short_subj = db.execute(sa_text(
        "SELECT brand, ROUND(AVG(LENGTH(subject)), 1) as avg_len, COUNT(*) as cnt "
        "FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL "
        "AND received_at >= :since "
        "GROUP BY brand HAVING COUNT(*) >= 5 "
        "ORDER BY avg_len ASC LIMIT 5"
    ), {"since": since}).fetchall()
    short_brands = ", ".join(f"{r[0]} (avg {r[1]} chars)" for r in short_subj) if short_subj else "N/A"

    # Brands that never use discount language
    no_discount = db.execute(sa_text(
        "SELECT brand, COUNT(*) as total "
        "FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL "
        "AND received_at >= :since "
        "GROUP BY brand "
        "HAVING COUNT(*) >= 5 "
        "AND SUM(CASE WHEN LOWER(subject) LIKE '%% off%%' OR LOWER(subject) LIKE '%%sale%%' "
        "OR LOWER(subject) LIKE '%%discount%%' THEN 1 ELSE 0 END) = 0 "
        "ORDER BY total DESC LIMIT 5"
    ), {"since": since}).fetchall()
    no_discount_brands = ", ".join(f"{r[0]} ({r[1]} emails)" for r in no_discount) if no_discount else "N/A"

    # Brands that never use exclamation marks
    no_excl = db.execute(sa_text(
        "SELECT brand, COUNT(*) as total "
        "FROM emails WHERE brand IS NOT NULL AND subject IS NOT NULL "
        "AND received_at >= :since "
        "GROUP BY brand "
        "HAVING COUNT(*) >= 5 "
        "AND SUM(CASE WHEN subject LIKE '%%!%%' THEN 1 ELSE 0 END) = 0 "
        "ORDER BY total DESC LIMIT 5"
    ), {"since": since}).fetchall()
    no_excl_brands = ", ".join(f"{r[0]} ({r[1]} emails)" for r in no_excl) if no_excl else "N/A"

    return (
        f"Original tweet: {tweet_text}\n"
        f"Author: @{author_handle}\n\n"
        f"=== CONTRARIAN DATA POINTS ===\n"
        f"Total brands tracked: {total_brands}\n"
        f"Brands that NEVER use exclamation marks: {no_excl_brands}\n"
        f"Brands with shortest subject lines: {short_brands}\n"
        f"Brands that NEVER mention discounts: {no_discount_brands}\n"
        f"\nUse whichever data point best contradicts the original tweet's assumption."
    )


def _build_reply_example(db: Session, **kwargs) -> str:
    """Fetch real brand email examples relevant to the tweet topic."""
    tweet_text = kwargs.get("tweet_text", "")
    author_handle = kwargs.get("author_handle", "")
    if not tweet_text:
        raise ValueError("reply_example requires 'tweet_text' parameter")

    since = _find_active_window(db, preferred_days=7)

    recent_emails = (
        db.query(Email.brand, Email.subject, Email.type)
        .filter(Email.received_at >= since, Email.subject.isnot(None), Email.brand.isnot(None))
        .order_by(func.random())
        .limit(15)
        .all()
    )

    examples = "\n".join(
        f"  - {e[0]} | Type: {e[2] or 'Unknown'} | Subject: \"{e[1][:80]}\""
        for e in recent_emails
    )

    return (
        f"Original tweet: {tweet_text}\n"
        f"Author: @{author_handle}\n\n"
        f"=== REAL EMAIL EXAMPLES (use the most relevant one) ===\n"
        f"{examples}\n\n"
        f"Pick the example that best relates to what the original tweet is discussing. "
        f"Use the REAL brand name and REAL subject line."
    )


def _build_reply_quick_tip(db: Session, **kwargs) -> str:
    """Build context for a tactical tip reply."""
    tweet_text = kwargs.get("tweet_text", "")
    author_handle = kwargs.get("author_handle", "")
    if not tweet_text:
        raise ValueError("reply_quick_tip requires 'tweet_text' parameter")

    since = _find_active_window(db, preferred_days=30)
    total_brands = db.query(func.count(func.distinct(Email.brand))).filter(
        Email.received_at >= since, Email.brand.isnot(None)
    ).scalar() or 0
    total_emails = db.query(func.count(Email.id)).filter(Email.received_at >= since).scalar() or 0

    subjects = db.query(Email.subject).filter(
        Email.received_at >= since, Email.subject.isnot(None)
    ).all()
    total = len(subjects)
    avg_len = round(sum(len(s) for (s,) in subjects) / max(total, 1))
    pct_personalized = round(sum(1 for (s,) in subjects if any(
        w in s.lower() for w in ["your ", "you ", "just for", "picked for"]
    )) / max(total, 1) * 100, 1)

    return (
        f"Original tweet: {tweet_text}\n"
        f"Author: @{author_handle}\n\n"
        f"Context: We track {total_brands}+ brands, {total_emails}+ emails.\n"
        f"Avg subject line: {avg_len} chars. {pct_personalized}% use personalization words.\n"
        f"Ground your tip in real patterns where possible."
    )


def _build_reply_agree_amplify(db: Session, **kwargs) -> str:
    """Build amplification data for an agree+amplify reply."""
    # Reuse the data_drop builder — same rich data, different system prompt
    return _build_reply_data_drop(db, **kwargs)


def _build_reply_resource_drop(db: Session, **kwargs) -> str:
    """Build context for a resource recommendation reply."""
    tweet_text = kwargs.get("tweet_text", "")
    author_handle = kwargs.get("author_handle", "")
    if not tweet_text:
        raise ValueError("reply_resource_drop requires 'tweet_text' parameter")

    total_brands = db.query(func.count(func.distinct(Email.brand))).filter(
        Email.brand.isnot(None)
    ).scalar() or 0
    total_emails = db.query(func.count(Email.id)).scalar() or 0

    return (
        f"Original tweet: {tweet_text}\n"
        f"Author: @{author_handle}\n\n"
        f"MailMuse capabilities:\n"
        f"- Tracks {total_brands}+ D2C/ecommerce brands' email campaigns\n"
        f"- {total_emails}+ emails in the database\n"
        f"- Browse real email designs, subject lines, and strategies\n"
        f"- Compare brands side-by-side\n"
        f"- Filter by industry, email type, brand\n"
        f"- URL: mailmuse.in"
    )


# ---------------------------------------------------------------------------
# Tweet type configuration — maps type name to config dict
# ---------------------------------------------------------------------------

# output_mode: "single" = one tweet, "thread" = reply chain, "variants" = multiple alternatives
_NEW_TWEET_TYPES = {
    "email_teardown": {
        "builder": _build_email_teardown,
        "system_prompt": P1_EMAIL_TEARDOWN_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 200,
        "output_mode": "single",
        "append_url": True,
    },
    "strategy_thread": {
        "builder": _build_strategy_thread,
        "system_prompt": P2_STRATEGY_THREAD_SYSTEM,
        "model": "claude-sonnet-4-20250514",
        "max_tokens": 2500,
        "output_mode": "thread",
        "append_url": False,
    },
    "blog_promo": {
        "builder": _build_blog_promo,
        "system_prompt": P3_BLOG_PROMO_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 800,
        "output_mode": "variants",
        "append_url": False,  # URL is embedded in the prompt via [link]
    },
    "smart_reply": {
        "builder": _build_smart_reply,
        "system_prompt": P4_SMART_REPLY_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 200,
        "output_mode": "single",
        "append_url": False,
    },
    "engagement_bait": {
        "builder": _build_engagement_bait,
        "system_prompt": P5_ENGAGEMENT_BAIT_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 600,
        "output_mode": "variants",
        "append_url": False,
    },
    "brand_comparison": {
        "builder": _build_brand_comparison,
        "system_prompt": P6_BRAND_COMPARISON_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 300,
        "output_mode": "single",
        "append_url": False,  # CTA is in the prompt
    },
    "weekly_roundup": {
        "builder": _build_weekly_roundup,
        "system_prompt": P7_WEEKLY_ROUNDUP_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 300,
        "output_mode": "single",
        "append_url": False,  # CTA is in the prompt
    },
    "newsjacking": {
        "builder": _build_newsjacking,
        "system_prompt": P8_NEWSJACKING_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 600,
        "output_mode": "variants",
        "append_url": False,
    },
    "subject_spotlight": {
        "builder": _build_subject_spotlight,
        "system_prompt": P9_SUBJECT_SPOTLIGHT_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 300,
        "output_mode": "single",
        "append_url": False,  # CTA is in the prompt
    },
    "follow_up_reply": {
        "builder": _build_follow_up_reply,
        "system_prompt": P10_FOLLOW_UP_REPLY_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 200,
        "output_mode": "single",
        "append_url": False,
    },
    # Reply Hub styles (P11–P16)
    "reply_data_drop": {
        "builder": _build_reply_data_drop,
        "system_prompt": P11_REPLY_DATA_DROP_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 800,
        "output_mode": "variants",
        "append_url": False,
    },
    "reply_contrarian": {
        "builder": _build_reply_contrarian,
        "system_prompt": P12_REPLY_CONTRARIAN_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 800,
        "output_mode": "variants",
        "append_url": False,
    },
    "reply_example": {
        "builder": _build_reply_example,
        "system_prompt": P13_REPLY_EXAMPLE_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 800,
        "output_mode": "variants",
        "append_url": False,
    },
    "reply_quick_tip": {
        "builder": _build_reply_quick_tip,
        "system_prompt": P14_REPLY_QUICK_TIP_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 800,
        "output_mode": "variants",
        "append_url": False,
    },
    "reply_agree_amplify": {
        "builder": _build_reply_agree_amplify,
        "system_prompt": P15_REPLY_AGREE_AMPLIFY_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 800,
        "output_mode": "variants",
        "append_url": False,
    },
    "reply_resource_drop": {
        "builder": _build_reply_resource_drop,
        "system_prompt": P16_REPLY_RESOURCE_DROP_SYSTEM,
        "model": "claude-haiku-4-5-20251001",
        "max_tokens": 800,
        "output_mode": "variants",
        "append_url": False,
    },
}


# Mapping of tweet types to their context-builder functions
_TWEET_TYPE_BUILDERS = {
    "daily_digest": _build_daily_digest,
    "weekly_digest": _build_weekly_digest,
    "brand_spotlight": _build_brand_spotlight,
    "subject_line_insight": _build_subject_line_insight,
    "viral_thread": _build_viral_thread,
}


def generate_tweet_content(
    tweet_type: str,
    db: Optional[Session] = None,
    **kwargs,
) -> str:
    """
    Generate tweet text for a given *tweet_type* using live DB data and Claude.

    For thread types (viral_thread, strategy_thread), returns multiple tweets
    separated by ``\\n---\\n``.
    For variant types (blog_promo, engagement_bait, newsjacking), returns
    multiple alternatives separated by ``\\n---\\n``.
    For all other types, returns a single tweet.

    Pass extra **kwargs for types that need manual input (e.g. smart_reply
    needs tweet_text, blog_promo needs url/title, etc.).
    """
    all_types = set(_TWEET_TYPE_BUILDERS.keys()) | set(_NEW_TWEET_TYPES.keys())
    if tweet_type not in all_types:
        raise ValueError(
            f"Unknown tweet_type '{tweet_type}'. "
            f"Valid types: {sorted(all_types)}"
        )

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        # --- New prompt-engine types (P1–P10) ---
        if tweet_type in _NEW_TWEET_TYPES:
            config = _NEW_TWEET_TYPES[tweet_type]
            builder = config["builder"]
            context = builder(db, **kwargs)

            result = _call_claude_for_tweet(
                system_prompt=config["system_prompt"],
                user_prompt=context,
                model=config["model"],
                max_tokens=config["max_tokens"],
            )

            # For single tweets with append_url, add the site URL
            if config["output_mode"] == "single" and config["append_url"]:
                if len(result) > MAX_TWEET_BODY_LEN:
                    result = result[: MAX_TWEET_BODY_LEN - 1] + "\u2026"
                result = result + SITE_URL

            return result

        # --- Legacy types (daily_digest, weekly_digest, etc.) ---
        builder = _TWEET_TYPE_BUILDERS[tweet_type]
        context = builder(db)

        if tweet_type == "viral_thread":
            return _generate_viral_thread(context)

        user_prompt = (
            f"Tweet type: {tweet_type}\n\n"
            f"Data context:\n{context}\n\n"
            f"Write a tweet (max {MAX_TWEET_BODY_LEN} characters). "
            "Do NOT include any URL — it will be appended automatically."
        )

        tweet_body = _call_claude_for_tweet(SYSTEM_PROMPT, user_prompt)

        if len(tweet_body) > MAX_TWEET_BODY_LEN:
            tweet_body = tweet_body[: MAX_TWEET_BODY_LEN - 1] + "\u2026"

        tweet = tweet_body + SITE_URL
        return tweet

    finally:
        if close_db:
            db.close()


_VIRAL_ANGLES = [
    {
        "framework": "Myth Buster",
        "instruction": "Use the MYTH BUSTER framework: Start by stating a common belief about email marketing, then demolish it with data. Make readers feel like everything they learned is wrong.",
    },
    {
        "framework": "Contrarian Take",
        "instruction": "Use the CONTRARIAN TAKE framework: Find a pattern where the data says the OPPOSITE of conventional wisdom. Lead with 'Everyone says X. The data says the opposite.'",
    },
    {
        "framework": "Bold Claim + Proof",
        "instruction": "Use the BOLD CLAIM + PROOF framework: Open with the most shocking single stat or finding, then spend the thread proving it with receipts and real examples.",
    },
    {
        "framework": "Data-Driven Surprise",
        "instruction": "Use the DATA-DRIVEN SURPRISE framework: Lead with the most counterintuitive stat you can find. Structure the thread as a countdown of surprising discoveries.",
    },
    {
        "framework": "Good vs Bad Comparison",
        "instruction": "Use the GOOD VS BAD COMPARISON framework: Pick 2-3 brands with opposite strategies and compare them head-to-head. Show how wildly different approaches can both work (or how one fails).",
    },
    {
        "framework": "Brand Deep-Dive",
        "instruction": "Pick ONE fascinating brand from the data and do a deep-dive thread on their email strategy. Analyze their subject lines, patterns, and what makes them unique. Make it feel like competitive intelligence people would pay for.",
    },
    {
        "framework": "Industry Secrets",
        "instruction": "Frame the thread as insider secrets from analyzing thousands of emails. Use 'I analyzed X emails and found Y things most marketers don't know.' Make each tweet a separate secret/finding.",
    },
]


def _generate_viral_thread(context: str) -> str:
    """
    Generate a viral Twitter thread using Claude Sonnet and the full
    viral frameworks prompt. Returns tweets separated by \\n---\\n.
    Randomly selects a framework/angle for variety.
    """
    client = _get_anthropic_client()

    angle = random.choice(_VIRAL_ANGLES)

    user_prompt = (
        f"FRAMEWORK TO USE: {angle['framework']}\n"
        f"{angle['instruction']}\n\n"
        "Using the database stats below, write a VIRAL Twitter thread "
        "(7-9 tweets). Pick the most surprising/counterintuitive data "
        "points.\n\n"
        f"{context}\n\n"
        "IMPORTANT: Make this thread DIFFERENT from a typical 'best brands "
        "break all the rules' angle. Find a FRESH take from the data.\n\n"
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


# ---------------------------------------------------------------------------
# Public helpers for main.py
# ---------------------------------------------------------------------------

def get_all_valid_types() -> list[str]:
    """Return all valid tweet type names (legacy + new)."""
    return sorted(set(_TWEET_TYPE_BUILDERS.keys()) | set(_NEW_TWEET_TYPES.keys()))


def get_output_mode(tweet_type: str) -> str:
    """Return the output mode for a tweet type: 'single', 'thread', or 'variants'."""
    if tweet_type in _NEW_TWEET_TYPES:
        return _NEW_TWEET_TYPES[tweet_type]["output_mode"]
    if tweet_type == "viral_thread":
        return "thread"
    return "single"
