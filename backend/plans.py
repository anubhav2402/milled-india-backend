"""
Centralized plan configuration — single source of truth for all tier limits.

Usage:
    from backend.plans import PLAN_LIMITS, get_effective_plan, check_limit
"""

from datetime import datetime
from typing import Optional

# Plan hierarchy (lower index = lower tier)
PLAN_HIERARCHY = ["free", "starter", "pro", "agency"]


def plan_rank(plan: str) -> int:
    """Return numeric rank for a plan. Higher = more powerful."""
    try:
        return PLAN_HIERARCHY.index(plan)
    except ValueError:
        return 0


def min_plan_for_feature(feature: str) -> str:
    """Return the minimum plan required for a feature."""
    for plan in PLAN_HIERARCHY:
        limit = PLAN_LIMITS[plan].get(feature)
        if limit is True or (isinstance(limit, (int, float)) and limit > 0):
            return plan
        if limit is None:  # None means unlimited
            return plan
    return "agency"


PLAN_LIMITS = {
    "free": {
        "archive_days": 30,
        "email_views_per_day": 20,
        "brand_pages_per_day": 5,
        "collections": 5,
        "emails_per_collection": 10,
        "html_exports_per_month": 0,
        "search_level": "basic",           # basic keyword only
        "analytics": "none",               # blurred preview
        "campaign_calendar": False,
        "alerts": 0,
        "seats": 1,
        "template_editor": "view_only",    # can open but not export
        "bulk_export": False,
        "downloadable_reports": False,
        "follows": 3,
        "bookmarks": 10,
    },
    "starter": {
        "archive_days": 180,
        "email_views_per_day": 75,
        "brand_pages_per_day": 25,
        "collections": 15,
        "emails_per_collection": 50,
        "html_exports_per_month": 3,
        "search_level": "advanced",        # industry, type filters
        "analytics": "basic",              # send frequency only
        "campaign_calendar": False,
        "alerts": 0,
        "seats": 1,
        "template_editor": "limited",      # edit + 3 exports/mo
        "bulk_export": False,
        "downloadable_reports": False,
        "follows": 10,
        "bookmarks": 50,
    },
    "pro": {
        "archive_days": None,              # None = unlimited
        "email_views_per_day": None,
        "brand_pages_per_day": None,
        "collections": None,
        "emails_per_collection": None,
        "html_exports_per_month": None,
        "search_level": "full",            # multi-parameter, boolean
        "analytics": "full",
        "campaign_calendar": True,
        "alerts": 5,
        "seats": 3,
        "template_editor": "unlimited",
        "bulk_export": False,
        "downloadable_reports": False,
        "follows": None,
        "bookmarks": None,
    },
    "agency": {
        "archive_days": None,
        "email_views_per_day": None,
        "brand_pages_per_day": None,
        "collections": None,
        "emails_per_collection": None,
        "html_exports_per_month": None,
        "search_level": "full",
        "analytics": "full",
        "campaign_calendar": True,
        "alerts": None,                    # unlimited
        "seats": 10,
        "template_editor": "unlimited",
        "bulk_export": True,
        "downloadable_reports": True,
        "follows": None,
        "bookmarks": None,
    },
}

# Pricing constants (in INR)
PLAN_PRICES = {
    "free": {"monthly": 0, "annual": 0},
    "starter": {"monthly": 599, "annual": 5999},
    "pro": {"monthly": 1599, "annual": 15999},
    "agency": {"monthly": 3999, "annual": 39999},
}


def get_effective_plan(user) -> str:
    """
    Determine the user's effective plan, accounting for trials and expiration.

    Priority:
    1. If user has a paid plan AND it hasn't expired → return that plan
    2. If user is within trial period → return "pro"
    3. Otherwise → return "free"
    """
    if user is None:
        return "free"

    # Check if they have an active paid subscription
    if user.subscription_tier in ("starter", "pro", "agency"):
        # If no expiry set, or expiry is in the future, plan is active
        if not user.subscription_expires_at or user.subscription_expires_at > datetime.utcnow():
            return user.subscription_tier

    # Check if within trial period
    if user.trial_ends_at and user.trial_ends_at > datetime.utcnow():
        return "pro"

    return "free"


def get_limit(plan: str, feature: str):
    """Get the limit for a specific feature on a plan. Returns None for unlimited."""
    return PLAN_LIMITS.get(plan, PLAN_LIMITS["free"]).get(feature)


def is_unlimited(plan: str, feature: str) -> bool:
    """Check if a feature is unlimited on a given plan."""
    return get_limit(plan, feature) is None


def check_numeric_limit(plan: str, feature: str, current_usage: int) -> dict:
    """
    Check if a user is within their numeric limit for a feature.

    Returns:
        {
            "allowed": bool,
            "limit": int or None (unlimited),
            "used": int,
            "remaining": int or None (unlimited),
            "upgrade_to": str or None (next tier that gives more)
        }
    """
    limit = get_limit(plan, feature)

    if limit is None:
        return {
            "allowed": True,
            "limit": None,
            "used": current_usage,
            "remaining": None,
            "upgrade_to": None,
        }

    allowed = current_usage < limit
    remaining = max(0, limit - current_usage)

    # Find the next tier that gives more
    upgrade_to = None
    current_rank = plan_rank(plan)
    for higher_plan in PLAN_HIERARCHY[current_rank + 1:]:
        higher_limit = get_limit(higher_plan, feature)
        if higher_limit is None or (isinstance(higher_limit, (int, float)) and higher_limit > limit):
            upgrade_to = higher_plan
            break

    return {
        "allowed": allowed,
        "limit": limit,
        "used": current_usage,
        "remaining": remaining,
        "upgrade_to": upgrade_to,
    }
