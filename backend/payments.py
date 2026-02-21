"""Razorpay payment integration for MailMuse Pro subscriptions."""

import os
import hmac
import hashlib
from datetime import datetime, timedelta

import razorpay

RAZORPAY_KEY_ID = os.getenv("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.getenv("RAZORPAY_KEY_SECRET", "")
RAZORPAY_PLAN_MONTHLY = os.getenv("RAZORPAY_PLAN_MONTHLY", "")
RAZORPAY_PLAN_ANNUAL = os.getenv("RAZORPAY_PLAN_ANNUAL", "")
RAZORPAY_WEBHOOK_SECRET = os.getenv("RAZORPAY_WEBHOOK_SECRET", "")


def get_razorpay_client():
    """Get configured Razorpay client."""
    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        raise ValueError("Razorpay credentials not configured")
    return razorpay.Client(auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET))


def create_subscription(user_email: str, plan: str = "monthly"):
    """Create a Razorpay subscription for a user.

    Args:
        user_email: User's email address
        plan: "monthly" or "annual"

    Returns:
        Razorpay subscription object with id, short_url, etc.
    """
    client = get_razorpay_client()

    plan_id = RAZORPAY_PLAN_MONTHLY if plan == "monthly" else RAZORPAY_PLAN_ANNUAL
    if not plan_id:
        raise ValueError(f"Razorpay {plan} plan ID not configured")

    subscription = client.subscription.create({
        "plan_id": plan_id,
        "total_count": 12 if plan == "monthly" else 1,
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
        "monthly": {
            "price": 2499,  # INR in paise = ₹2,499 (~$29)
            "display_price": "₹2,499",
            "period": "month",
            "plan_id": RAZORPAY_PLAN_MONTHLY,
        },
        "annual": {
            "price": 19188,  # INR in paise = ₹19,188 (~$228, i.e. $19/mo)
            "display_price": "₹19,188",
            "period": "year",
            "savings": "₹10,800/year",
            "plan_id": RAZORPAY_PLAN_ANNUAL,
        },
        "razorpay_key_id": RAZORPAY_KEY_ID,
    }
