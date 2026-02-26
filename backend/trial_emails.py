"""
Trial email reminder system for MailMuse.

Sends reminder emails to users whose trial is about to expire:
- Day 10 (4 days left): "Your Pro trial ends in 4 days"
- Day 13 (1 day left): "Tomorrow is the last day"
- Day 15 (1 day after expiry): "Your trial ended"

Usage:
    python -m backend.trial_emails
"""

import os
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

from sqlalchemy.orm import Session
from .db import SessionLocal
from . import models

# SMTP configuration (Gmail App Password or any SMTP relay)
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
FROM_EMAIL = os.getenv("FROM_EMAIL", "hello@mailmuse.in")
FROM_NAME = os.getenv("FROM_NAME", "MailMuse")

SITE_URL = os.getenv("SITE_URL", "https://www.mailmuse.in")


def _send_email(to_email: str, subject: str, html_body: str) -> bool:
    """Send an HTML email via SMTP."""
    if not SMTP_USER or not SMTP_PASSWORD:
        print(f"  [SKIP] SMTP not configured, would send to {to_email}: {subject}")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = f"{FROM_NAME} <{FROM_EMAIL}>"
    msg["To"] = to_email

    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(FROM_EMAIL, to_email, msg.as_string())
        print(f"  [SENT] {subject} -> {to_email}")
        return True
    except Exception as e:
        print(f"  [ERROR] Failed to send to {to_email}: {e}")
        return False


def _email_template(title: str, body_html: str, cta_text: str = "View Plans", cta_url: str = "") -> str:
    """Generate a branded HTML email template."""
    if not cta_url:
        cta_url = f"{SITE_URL}/pricing"

    return f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>
<body style="margin:0;padding:0;background:#faf9f7;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;">
<div style="max-width:560px;margin:0 auto;padding:40px 20px;">
  <div style="text-align:center;margin-bottom:32px;">
    <a href="{SITE_URL}" style="text-decoration:none;font-size:24px;font-weight:700;color:#1a1a1a;letter-spacing:-0.5px;">
      Mail<span style="color:#c45a3c;">Muse</span>
    </a>
  </div>
  <div style="background:white;border-radius:12px;padding:32px 28px;border:1px solid #e5e5e5;">
    <h1 style="font-size:22px;font-weight:600;color:#1a1a1a;margin:0 0 16px;">{title}</h1>
    {body_html}
    <div style="text-align:center;margin-top:28px;">
      <a href="{cta_url}" style="display:inline-block;background:linear-gradient(135deg,#c45a3c,#a0452e);color:white;padding:12px 32px;border-radius:10px;text-decoration:none;font-weight:600;font-size:15px;">
        {cta_text}
      </a>
    </div>
  </div>
  <div style="text-align:center;margin-top:24px;font-size:12px;color:#999;">
    <p>You're receiving this because you signed up for MailMuse.</p>
    <p><a href="{SITE_URL}" style="color:#999;">mailmuse.in</a></p>
  </div>
</div>
</body>
</html>"""


def _get_sent_emails(user: models.User) -> set:
    """Get set of reminder keys already sent to this user."""
    try:
        return set(json.loads(user.trial_emails_sent or "[]"))
    except (json.JSONDecodeError, TypeError):
        return set()


def _mark_sent(db: Session, user: models.User, key: str):
    """Mark a reminder key as sent."""
    sent = _get_sent_emails(user)
    sent.add(key)
    user.trial_emails_sent = json.dumps(list(sent))
    db.commit()


def send_trial_reminders():
    """Main entry point: check all users and send applicable trial reminders."""
    db: Session = SessionLocal()
    now = datetime.utcnow()

    print(f"[Trial Emails] Starting at {now.isoformat()}")

    # Find users with trial_ends_at set
    users = db.query(models.User).filter(
        models.User.trial_ends_at.isnot(None),
        # Only users who haven't paid (still on free tier)
        models.User.subscription_tier.in_(["free", None]),
    ).all()

    print(f"  Found {len(users)} users with active/recent trials")

    sent_count = 0

    for user in users:
        if not user.trial_ends_at:
            continue

        days_until_expiry = (user.trial_ends_at - now).days
        sent_keys = _get_sent_emails(user)

        # Day 10 reminder (4 days left)
        if days_until_expiry <= 4 and days_until_expiry > 1 and "day10" not in sent_keys:
            subject = f"Your Pro trial ends in {days_until_expiry} days"
            body = f"""
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Hi{(' ' + user.name.split()[0]) if user.name else ''},
            </p>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Your 14-day Pro trial is ending soon — just <strong>{days_until_expiry} days left</strong>.
            </p>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Here's what you'll lose access to when it expires:
            </p>
            <ul style="font-size:14px;color:#555;line-height:1.8;padding-left:20px;">
              <li>Full email archive (will drop to 30 days)</li>
              <li>Unlimited email views (will drop to 20/day)</li>
              <li>Analytics & benchmarks</li>
              <li>Campaign calendar</li>
              <li>Template editor & exports</li>
            </ul>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Upgrade now to keep everything — plans start at just ₹599/month.
            </p>
            """
            if _send_email(user.email, subject, _email_template(subject, body, "Upgrade Now")):
                _mark_sent(db, user, "day10")
                sent_count += 1

        # Day 13 reminder (1 day left)
        elif days_until_expiry <= 1 and days_until_expiry > 0 and "day13" not in sent_keys:
            subject = "Tomorrow is the last day of your Pro trial"
            body = f"""
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Hi{(' ' + user.name.split()[0]) if user.name else ''},
            </p>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Your Pro trial expires <strong>tomorrow</strong>. After that, your account will revert to the Free plan.
            </p>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              If you've been enjoying analytics, the full archive, and unlimited access — now's the time to upgrade.
            </p>
            <p style="font-size:14px;color:#666;line-height:1.6;background:#faf5f2;padding:16px;border-radius:8px;">
              <strong>Starter:</strong> ₹599/mo — 6-month archive, 75 views/day, advanced search<br/>
              <strong>Pro:</strong> ₹1,599/mo — Full archive, unlimited everything, analytics<br/>
              <em>Annual plans save 17%.</em>
            </p>
            """
            if _send_email(user.email, subject, _email_template(subject, body, "Choose a Plan")):
                _mark_sent(db, user, "day13")
                sent_count += 1

        # Day 15 reminder (1 day after expiry)
        elif days_until_expiry <= -1 and days_until_expiry > -3 and "day15" not in sent_keys:
            subject = "Your Pro trial has ended — here's what's next"
            body = f"""
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Hi{(' ' + user.name.split()[0]) if user.name else ''},
            </p>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              Your Pro trial ended yesterday. Your account is now on the Free plan.
            </p>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              You can still browse the last 30 days of emails and view up to 20 per day. But if you miss having the full archive, analytics, and unlimited access — upgrading takes just a minute.
            </p>
            <p style="font-size:15px;color:#444;line-height:1.6;">
              We'd love to have you back on Pro. Plans start at ₹599/month with a 7-day money-back guarantee.
            </p>
            """
            if _send_email(user.email, subject, _email_template(subject, body, "Upgrade Now")):
                _mark_sent(db, user, "day15")
                sent_count += 1

    db.close()
    print(f"[Trial Emails] Done. Sent {sent_count} emails.")


if __name__ == "__main__":
    send_trial_reminders()
