"""Razorpay payment integration for MailMuse Pro subscriptions."""

import os
import hmac
import hashlib
from datetime import datetime, timedelta

import razorpay

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
# Legacy Pro plan IDs (backward compat for existing subscribers)
RAZORPAY_PLAN_MONTHLY = os.getenv("RAZORPAY_PLAN_MONTHLY", "")
RAZORPAY_PLAN_ANNUAL = os.getenv("RAZORPAY_PLAN_ANNUAL", "")

# New per-tier plan IDs
RAZORPAY_PLAN_STARTER_MONTHLY = os.getenv("RAZORPAY_PLAN_STARTER_MONTHLY", "")
RAZORPAY_PLAN_STARTER_ANNUAL = os.getenv("RAZORPAY_PLAN_STARTER_ANNUAL", "")
RAZORPAY_PLAN_PRO_MONTHLY = os.getenv("RAZORPAY_PLAN_PRO_MONTHLY", RAZORPAY_PLAN_MONTHLY)
RAZORPAY_PLAN_PRO_ANNUAL = os.getenv("RAZORPAY_PLAN_PRO_ANNUAL", RAZORPAY_PLAN_ANNUAL)
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")


def get_razorpay_client():
    """Get configured Razorpay client."""
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        raise ValueError("Razorpay credentials not configured")
    return razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))


def create_subscription(user_email: str, plan: str = "monthly", tier: str = "pro"):
    """Create a Razorpay subscription for a user.

    Args:
        user_email: User's email address
        plan: "monthly" or "annual"
        tier: "starter" or "pro"

    Returns:
        Razorpay subscription object with id, short_url, etc.
    """
    client = get_razorpay_client()

    # Map tier + billing to Razorpay plan ID
    plan_map = {
        ("starter", "monthly"): RAZORPAY_PLAN_STARTER_MONTHLY,
        ("starter", "annual"): RAZORPAY_PLAN_STARTER_ANNUAL,
        ("pro", "monthly"): RAZORPAY_PLAN_PRO_MONTHLY,
        ("pro", "annual"): RAZORPAY_PLAN_PRO_ANNUAL,
    }

    plan_id = plan_map.get((tier, plan))
    if not plan_id:
        raise ValueError(f"Razorpay plan not configured for {tier}/{plan}")

    total_count = 12 if plan == "monthly" else 1
    subscription = client.subscription.create({
        "plan_id": plan_id,
        "total_count": total_count,
        "quantity": 1,
        "notify_info": {
            "notify_email": user_email,
        },
    })

    return subscription


def verify_payment_signature(payment_id: str, subscription_id: str, signature: str) -> bool:
    """Verify Razorpay payment signature.

    Args:
        payment_id: Razorpay payment ID
        subscription_id: Razorpay subscription ID
        signature: Razorpay signature to verify

    Returns:
        True if signature is valid
    """
    message = f"{payment_id}|{subscription_id}"
    expected = hmac.new(
        RAZORPAY_KEY_SECRET.encode(),
        message.encode(),
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def verify_webhook_signature(body: bytes, signature: str) -> bool:
    """Verify Razorpay webhook signature.

    Args:
        body: Raw request body bytes
        signature: X-Razorpay-Signature header value

    Returns:
        True if signature is valid
    """
    if not RAZORPAY_WEBHOOK_SECRET:
        return False
    expected = hmac.new(
        RAZORPAY_WEBHOOK_SECRET.encode(),
        body,
        hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


def get_subscription_details(subscription_id: str):
    """Fetch subscription details from Razorpay."""
    client = get_razorpay_client()
    return client.subscription.fetch(subscription_id)


def cancel_subscription(subscription_id: str):
    """Cancel a Razorpay subscription."""
    client = get_razorpay_client()
    return client.subscription.cancel(subscription_id)


def get_plan_details():
    """Return plan pricing info for the frontend."""
    return {
        "plans": {
            "free": {
                "name": "Free",
                "monthly_price": 0,
                "annual_price": 0,
                "display_monthly": "₹0",
                "display_annual": "₹0",
            },
            "starter": {
                "name": "Starter",
                "monthly_price": 599,
                "annual_price": 5999,
                "display_monthly": "₹599",
                "display_annual": "₹5,999",
                "monthly_plan_id": RAZORPAY_PLAN_STARTER_MONTHLY,
                "annual_plan_id": RAZORPAY_PLAN_STARTER_ANNUAL,
            },
            "pro": {
                "name": "Pro",
                "monthly_price": 1599,
                "annual_price": 15999,
                "display_monthly": "₹1,599",
                "display_annual": "₹15,999",
                "monthly_plan_id": RAZORPAY_PLAN_PRO_MONTHLY,
                "annual_plan_id": RAZORPAY_PLAN_PRO_ANNUAL,
            },
            "agency": {
                "name": "Agency",
                "monthly_price": 3999,
                "annual_price": 39999,
                "display_monthly": "₹3,999",
                "display_annual": "₹39,999",
                "contact_sales": True,
            },
        },
        "razorpay_key_id": RAZORPAY_KEY_ID,
    }
