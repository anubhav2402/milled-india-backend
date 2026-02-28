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

def _get_openai_client():
    """Get OpenAI client with API key from environment."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    from openai import OpenAI
    return OpenAI(api_key=api_key)


def _call_openai_for_tweet(system_prompt: str, user_prompt: str) -> str:
    """
    Ask GPT-4o-mini to write a tweet.  Returns the raw text from the model.

    Uses low temperature for consistency (same pattern as ai_classifier).
    """
    client = _get_openai_client()

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        temperature=0.2,
        max_tokens=200,
        timeout=25,
    )

    result_text = response.choices[0].message.content.strip()

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
MAX_TWEET_BODY_LEN = 270  # leave room for the appended URL

SYSTEM_PROMPT = (
    "You are a witty, concise social-media copywriter for MailMuse, "
    "an email marketing intelligence platform tracking 300+ brands worldwide. "
    "Write a single tweet (max 270 characters, no hashtags unless they "
    "feel natural). Be insightful, data-driven, and engaging. "
    "Return ONLY the tweet text — no quotes, no labels, no extra formatting."
)


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


# Mapping of tweet types to their context-builder functions
_TWEET_TYPE_BUILDERS = {
    "daily_digest": _build_daily_digest,
    "weekly_digest": _build_weekly_digest,
    "brand_spotlight": _build_brand_spotlight,
    "subject_line_insight": _build_subject_line_insight,
}


def generate_tweet_content(tweet_type: str, db: Optional[Session] = None) -> str:
    """
    Generate tweet text for a given *tweet_type* using live DB data and OpenAI.

    Parameters
    ----------
    tweet_type : str
        One of "daily_digest", "weekly_digest", "brand_spotlight",
        "subject_line_insight".
    db : Session, optional
        An existing SQLAlchemy session. If ``None`` a new session is created
        (and closed afterwards).

    Returns
    -------
    str
        The complete tweet text, including the trailing MailMuse URL.
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

        # 2. Call OpenAI to craft a tweet
        user_prompt = (
            f"Tweet type: {tweet_type}\n\n"
            f"Data context:\n{context}\n\n"
            f"Write a tweet (max {MAX_TWEET_BODY_LEN} characters). "
            "Do NOT include any URL — it will be appended automatically."
        )

        tweet_body = _call_openai_for_tweet(SYSTEM_PROMPT, user_prompt)

        # Ensure the body fits within the limit
        if len(tweet_body) > MAX_TWEET_BODY_LEN:
            tweet_body = tweet_body[: MAX_TWEET_BODY_LEN - 1] + "\u2026"

        # 3. Append the site URL
        tweet = tweet_body + SITE_URL
        return tweet

    finally:
        if close_db:
            db.close()
