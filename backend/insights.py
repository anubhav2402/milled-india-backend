"""
Insights Engine — Tier 1.

Computes weekly intelligence from the email database:
  1. Weekly Scoreboard — key metrics week-over-week
  2. Brand Sequences — coordinated email campaigns detected
  3. Subject Line Formulas — classify and track formula trends
  4. New Tactic Detector — flag per-brand signal deviations

Usage:
    from backend.insights import compute_all
    results = compute_all(db)

    # Or individually:
    from backend.insights import compute_weekly_scoreboard
    data = compute_weekly_scoreboard(db)
"""

import json
import re
from datetime import datetime, timedelta
from collections import Counter, defaultdict

from sqlalchemy.orm import Session

from .models import Email, InsightCache


# ── Helpers ──────────────────────────────────────────────────────────────────


def _week_boundaries(reference_date: datetime = None):
    """Return (this_monday, this_sunday, last_monday, last_sunday)."""
    ref = reference_date or datetime.utcnow()
    this_monday = (ref - timedelta(days=ref.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    this_sunday = this_monday + timedelta(days=7)
    last_monday = this_monday - timedelta(days=7)
    last_sunday = this_monday
    return this_monday, this_sunday, last_monday, last_sunday


def _week_label(reference_date: datetime = None) -> str:
    """Return ISO week string like '2026-W10'."""
    ref = reference_date or datetime.utcnow()
    iso = ref.isocalendar()
    return f"{iso[0]}-W{iso[1]:02d}"


def _trailing_weeks(reference_date: datetime = None, n_weeks: int = 4):
    """Return list of (start, end) tuples for n trailing weeks BEFORE current week."""
    ref = reference_date or datetime.utcnow()
    this_monday = (ref - timedelta(days=ref.weekday())).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    weeks = []
    for i in range(1, n_weeks + 1):
        end = this_monday - timedelta(days=7 * (i - 1))
        start = end - timedelta(days=7)
        weeks.append((start, end))
    return weeks


DISCOUNT_KEYWORDS = ["off", "%", "sale", "discount", "save"]


def _is_discount(subject: str) -> bool:
    lower = subject.lower()
    return any(kw in lower for kw in DISCOUNT_KEYWORDS)


_EMOJI_RE = re.compile(
    "["
    "\U0001F600-\U0001F64F"
    "\U0001F300-\U0001F5FF"
    "\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF"
    "\U00002702-\U000027B0"
    "\U000024C2-\U0001F251"
    "\U00002600-\U000026FF"
    "\U00002B05-\U00002B55"
    "\U0000FE00-\U0000FE0F"
    "\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F"
    "\U0001FA70-\U0001FAFF"
    "\U00002194-\U00002199"
    "\U000021A9-\U000021AA"
    "\U0000231A-\U0000231B"
    "\U00002328"
    "\U000023CF"
    "\U000023E9-\U000023F3"
    "\U000023F8-\U000023FA"
    "]+",
    flags=re.UNICODE,
)


def _has_emoji(text: str) -> bool:
    return bool(_EMOJI_RE.search(text))


def _pct(num, denom) -> float:
    if denom == 0:
        return 0.0
    return round(num / denom * 100, 1)


def _pct_change(current, previous) -> float:
    if previous == 0:
        return 100.0 if current > 0 else 0.0
    return round((current - previous) / previous * 100, 1)


# ── Insight 1: Weekly Scoreboard ─────────────────────────────────────────────


def compute_weekly_scoreboard(db: Session, reference_date: datetime = None) -> dict:
    """Key metrics compared week-over-week."""
    ref = reference_date or datetime.utcnow()
    this_start, this_end, last_start, last_end = _week_boundaries(ref)

    emails_this = db.query(Email).filter(
        Email.received_at >= this_start,
        Email.received_at < this_end,
        Email.subject.isnot(None),
    ).all()

    emails_last = db.query(Email).filter(
        Email.received_at >= last_start,
        Email.received_at < last_end,
        Email.subject.isnot(None),
    ).all()

    def _week_stats(emails):
        if not emails:
            return {
                "total": 0, "avg_subject_len": 0, "discount_pct": 0,
                "emoji_pct": 0, "weekend_pct": 0, "busiest_day": None,
                "brand_counts": {},
            }
        subjects = [e.subject for e in emails]
        total = len(emails)
        avg_len = round(sum(len(s) for s in subjects) / total, 1)
        discount_count = sum(1 for s in subjects if _is_discount(s))
        emoji_count = sum(1 for s in subjects if _has_emoji(s))
        weekend_count = sum(1 for e in emails if e.received_at and e.received_at.weekday() >= 5)

        day_counter = Counter(
            e.received_at.weekday() for e in emails if e.received_at
        )
        day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        busiest_day = day_names[day_counter.most_common(1)[0][0]] if day_counter else None

        brand_counter = Counter(
            e.brand for e in emails if e.brand and e.brand != "Unknown"
        )

        return {
            "total": total,
            "avg_subject_len": avg_len,
            "discount_pct": _pct(discount_count, total),
            "emoji_pct": _pct(emoji_count, total),
            "weekend_pct": _pct(weekend_count, total),
            "busiest_day": busiest_day,
            "brand_counts": dict(brand_counter),
        }

    this_stats = _week_stats(emails_this)
    last_stats = _week_stats(emails_last)

    # Biggest mover: brand with largest % increase, min 3 emails
    biggest_mover = None
    biggest_mover_change = 0
    for brand, this_count in this_stats["brand_counts"].items():
        last_count = last_stats["brand_counts"].get(brand, 0)
        change = _pct_change(this_count, last_count)
        if this_count >= 3 and change > biggest_mover_change:
            biggest_mover = brand
            biggest_mover_change = change

    return {
        "insight_type": "weekly_scoreboard",
        "week": _week_label(ref),
        "this_week": {"start": this_start.isoformat(), "end": this_end.isoformat()},
        "metrics": {
            "total_emails": {
                "this_week": this_stats["total"],
                "last_week": last_stats["total"],
                "change_pct": _pct_change(this_stats["total"], last_stats["total"]),
            },
            "avg_subject_length": {
                "this_week": this_stats["avg_subject_len"],
                "last_week": last_stats["avg_subject_len"],
                "change_pct": _pct_change(this_stats["avg_subject_len"], last_stats["avg_subject_len"]),
            },
            "discount_mention_pct": {
                "this_week": this_stats["discount_pct"],
                "last_week": last_stats["discount_pct"],
                "change_pct": _pct_change(this_stats["discount_pct"], last_stats["discount_pct"]),
            },
            "emoji_usage_pct": {
                "this_week": this_stats["emoji_pct"],
                "last_week": last_stats["emoji_pct"],
                "change_pct": _pct_change(this_stats["emoji_pct"], last_stats["emoji_pct"]),
            },
            "weekend_emails_pct": {
                "this_week": this_stats["weekend_pct"],
                "last_week": last_stats["weekend_pct"],
                "change_pct": _pct_change(this_stats["weekend_pct"], last_stats["weekend_pct"]),
            },
            "busiest_day": {
                "this_week": this_stats["busiest_day"],
                "last_week": last_stats["busiest_day"],
            },
            "biggest_mover": {
                "brand": biggest_mover,
                "change_pct": biggest_mover_change,
                "this_week_count": this_stats["brand_counts"].get(biggest_mover, 0) if biggest_mover else 0,
                "last_week_count": last_stats["brand_counts"].get(biggest_mover, 0) if biggest_mover else 0,
            },
        },
    }


# ── Insight 2: Brand Sequence Tracker ────────────────────────────────────────


def compute_brand_sequences(db: Session, reference_date: datetime = None) -> dict:
    """Detect 3+ emails from same brand within 7 days."""
    ref = reference_date or datetime.utcnow()
    this_start, this_end, _, _ = _week_boundaries(ref)

    # Widen window to 14 days to catch sequences starting last week
    window_start = this_start - timedelta(days=7)

    emails = db.query(Email).filter(
        Email.received_at >= window_start,
        Email.received_at < this_end,
        Email.brand.isnot(None),
        Email.brand != "Unknown",
        Email.subject.isnot(None),
    ).order_by(Email.brand, Email.received_at).all()

    # Group by brand
    brand_emails = defaultdict(list)
    for e in emails:
        brand_emails[e.brand].append(e)

    sequences = []
    for brand, brand_list in brand_emails.items():
        if len(brand_list) < 3:
            continue

        # Sliding window: find longest cluster of 3+ within 7 days
        best_cluster = []
        for i in range(len(brand_list)):
            cluster = [brand_list[i]]
            for j in range(i + 1, len(brand_list)):
                gap = (brand_list[j].received_at - brand_list[i].received_at).days
                if gap <= 7:
                    cluster.append(brand_list[j])
                else:
                    break

            if len(cluster) >= 3 and len(cluster) > len(best_cluster):
                best_cluster = cluster

        if len(best_cluster) < 3:
            continue

        # Build sequence entry
        seq_emails = []
        for k, e in enumerate(best_cluster):
            gap_hours = None
            if k > 0:
                delta = e.received_at - best_cluster[k - 1].received_at
                gap_hours = round(delta.total_seconds() / 3600, 1)
            seq_emails.append({
                "subject": e.subject,
                "type": e.type,
                "sent_at": e.received_at.isoformat(),
                "gap_hours_from_previous": gap_hours,
            })

        span_days = (best_cluster[-1].received_at - best_cluster[0].received_at).days

        # Keyword overlap between subject lines
        stop = {"the", "and", "for", "you", "your", "are", "our", "this",
                "that", "with", "from", "has", "have", "not", "all", "new"}
        all_words = []
        for e in best_cluster:
            words = set(re.findall(r"\b[a-zA-Z]{3,}\b", e.subject.lower()))
            all_words.append(words - stop)

        common = all_words[0]
        for ws in all_words[1:]:
            common = common & ws
        keyword_overlap = list(common)[:5]

        sequences.append({
            "brand": brand,
            "email_count": len(best_cluster),
            "span_days": span_days,
            "keyword_overlap": keyword_overlap,
            "emails": seq_emails,
        })

    # Sort by email count descending, take top 20
    sequences.sort(key=lambda s: s["email_count"], reverse=True)
    sequences = sequences[:20]

    return {
        "insight_type": "brand_sequences",
        "week": _week_label(ref),
        "window": {"start": window_start.isoformat(), "end": this_end.isoformat()},
        "total_sequences_found": len(sequences),
        "sequences": sequences,
    }


# ── Insight 3: Subject Line Formula Tracker ──────────────────────────────────


FORMULA_PATTERNS = {
    "discount_led": re.compile(
        r"(?:^\d+%\s*off|\bflat\s+\d+%|\bbuy\s+\d+\s+get|\bsave\s+\d+|\bfree\s+(?:shipping|delivery)|\bunder\s+[\u20b9$]|\bunder\s+rs)",
        re.IGNORECASE,
    ),
    "urgency": re.compile(
        r"(?:last chance|ends?\s+(?:tonight|today|soon|now)|hurry|final\s+(?:hours?|day)|limited\s+time|don't\s+miss|closing|expir)",
        re.IGNORECASE,
    ),
    "number_list": re.compile(
        r"\b\d+\b.*(?:ways|tips|looks|reasons|things|styles|ideas|steps|picks|must|best|top)",
        re.IGNORECASE,
    ),
    "personalization": re.compile(
        r"(?:^your\b|^you\b|for you|picked for|just for|waiting for you|your\s+(?:picks|style|cart|order|favorites|wishlist))",
        re.IGNORECASE,
    ),
    "curiosity_teaser": re.compile(
        r"(?:something\s+(?:big|new|special)|guess what|you won't believe|sneak peek|coming soon|just dropped|wait till you see|big reveal|surprise)",
        re.IGNORECASE,
    ),
    "social_proof": re.compile(
        r"(?:everyone|bestsell|most\s+(?:loved|wanted|popular)|trending|selling fast|going fast|fan favou?rite|crowd favou?rite)",
        re.IGNORECASE,
    ),
    "question": re.compile(r"\?"),
    "command": re.compile(
        r"^(?:shop|get|grab|check out|discover|try|meet|explore|stock up|treat yourself|upgrade|don't|do not)\b",
        re.IGNORECASE,
    ),
}

# Priority order for classification (first match wins)
FORMULA_PRIORITY = [
    "discount_led", "urgency", "number_list", "personalization",
    "curiosity_teaser", "social_proof", "question", "command",
]


def _classify_formula(subject: str) -> str:
    """Classify a subject line into a formula category."""
    for formula_name in FORMULA_PRIORITY:
        if FORMULA_PATTERNS[formula_name].search(subject):
            return formula_name
    return "other"


def compute_subject_formulas(db: Session, reference_date: datetime = None) -> dict:
    """Classify subject lines into formula categories, track trends."""
    ref = reference_date or datetime.utcnow()
    this_start, this_end, _, _ = _week_boundaries(ref)
    trailing = _trailing_weeks(ref, n_weeks=4)

    # This week's subjects
    this_emails = db.query(Email.subject).filter(
        Email.received_at >= this_start,
        Email.received_at < this_end,
        Email.subject.isnot(None),
    ).all()

    this_total = len(this_emails)
    this_formulas = Counter(_classify_formula(s[0]) for s in this_emails)

    # Trailing 4-week average
    trailing_totals = []
    trailing_formulas = Counter()
    for w_start, w_end in trailing:
        week_emails = db.query(Email.subject).filter(
            Email.received_at >= w_start,
            Email.received_at < w_end,
            Email.subject.isnot(None),
        ).all()
        trailing_totals.append(len(week_emails))
        for (s,) in week_emails:
            trailing_formulas[_classify_formula(s)] += 1

    n_trailing_weeks = len([t for t in trailing_totals if t > 0]) or 1
    avg_total = sum(trailing_totals) / n_trailing_weeks if trailing_totals else 1

    # Build distribution and trends
    all_formulas = sorted(set(list(this_formulas.keys()) + list(trailing_formulas.keys())))
    distribution = {}
    for formula in all_formulas:
        this_count = this_formulas.get(formula, 0)
        this_pct = _pct(this_count, this_total)
        avg_count = trailing_formulas.get(formula, 0) / n_trailing_weeks
        avg_pct = _pct(avg_count, avg_total) if avg_total > 0 else 0

        trend_pct = _pct_change(this_pct, avg_pct) if avg_pct > 0 else (100.0 if this_pct > 0 else 0.0)

        trend = "stable"
        if trend_pct >= 20:
            trend = "rising"
        elif trend_pct <= -20:
            trend = "falling"

        distribution[formula] = {
            "this_week_count": this_count,
            "this_week_pct": this_pct,
            "trailing_4wk_avg_pct": avg_pct,
            "change_pct": trend_pct,
            "trend": trend,
        }

    # Top 3 examples per formula
    examples = defaultdict(list)
    for (s,) in this_emails:
        f = _classify_formula(s)
        if len(examples[f]) < 3:
            examples[f].append(s)

    return {
        "insight_type": "subject_formulas",
        "week": _week_label(ref),
        "this_week_total_subjects": this_total,
        "distribution": distribution,
        "examples": dict(examples),
        "rising": [f for f, d in distribution.items() if d["trend"] == "rising"],
        "falling": [f for f, d in distribution.items() if d["trend"] == "falling"],
    }


# ── Insight 4: New Tactic Detector ───────────────────────────────────────────


_PERSONALIZATION_RE = re.compile(
    r"(?:^your\b|^you\b|for you|picked for|just for|waiting for you)",
    re.IGNORECASE,
)


def compute_new_tactics(db: Session, reference_date: datetime = None) -> dict:
    """Per brand, detect signal rate deviations from trailing 4-week average."""
    ref = reference_date or datetime.utcnow()
    this_start, this_end, _, _ = _week_boundaries(ref)
    trailing = _trailing_weeks(ref, n_weeks=4)

    # This week's emails by brand
    this_emails = db.query(Email).filter(
        Email.received_at >= this_start,
        Email.received_at < this_end,
        Email.brand.isnot(None),
        Email.brand != "Unknown",
        Email.subject.isnot(None),
    ).all()

    brand_this = defaultdict(list)
    for e in this_emails:
        brand_this[e.brand].append(e.subject)

    # Trailing 4 weeks by brand
    brand_trailing = defaultdict(list)
    for w_start, w_end in trailing:
        week_emails = db.query(Email).filter(
            Email.received_at >= w_start,
            Email.received_at < w_end,
            Email.brand.isnot(None),
            Email.brand != "Unknown",
            Email.subject.isnot(None),
        ).all()
        for e in week_emails:
            brand_trailing[e.brand].append(e.subject)

    def _brand_signals(subjects):
        n = len(subjects)
        if n == 0:
            return None
        return {
            "emoji_rate": _pct(sum(1 for s in subjects if _has_emoji(s)), n),
            "discount_rate": _pct(sum(1 for s in subjects if _is_discount(s)), n),
            "question_rate": _pct(sum(1 for s in subjects if "?" in s), n),
            "personalization_rate": _pct(
                sum(1 for s in subjects if _PERSONALIZATION_RE.search(s)), n
            ),
            "avg_subject_length": round(sum(len(s) for s in subjects) / n, 1),
        }

    alerts = []
    for brand, this_subjects in brand_this.items():
        if len(this_subjects) < 3:
            continue

        trailing_subjects = brand_trailing.get(brand, [])
        if len(trailing_subjects) < 5:
            continue

        this_signals = _brand_signals(this_subjects)
        trailing_signals = _brand_signals(trailing_subjects)
        if not this_signals or not trailing_signals:
            continue

        brand_alerts = []
        for signal_name in ["emoji_rate", "discount_rate", "question_rate",
                            "personalization_rate", "avg_subject_length"]:
            this_val = this_signals[signal_name]
            trail_val = trailing_signals[signal_name]

            if trail_val == 0 and this_val > 0:
                brand_alerts.append({
                    "signal": signal_name,
                    "this_week": this_val,
                    "trailing_avg": trail_val,
                    "change_type": "appeared",
                    "change_pct": None,
                })
            elif trail_val > 0 and this_val == 0:
                brand_alerts.append({
                    "signal": signal_name,
                    "this_week": this_val,
                    "trailing_avg": trail_val,
                    "change_type": "disappeared",
                    "change_pct": None,
                })
            elif trail_val > 0:
                change = _pct_change(this_val, trail_val)
                if abs(change) > 50:
                    brand_alerts.append({
                        "signal": signal_name,
                        "this_week": this_val,
                        "trailing_avg": trail_val,
                        "change_type": "spike" if change > 0 else "drop",
                        "change_pct": change,
                    })

        if brand_alerts:
            alerts.append({
                "brand": brand,
                "email_count_this_week": len(this_subjects),
                "email_count_trailing": len(trailing_subjects),
                "alerts": brand_alerts,
            })

    # Sort by number of alerts descending, then by email count
    alerts.sort(key=lambda a: (-len(a["alerts"]), -a["email_count_this_week"]))
    alerts = alerts[:30]

    return {
        "insight_type": "new_tactics",
        "week": _week_label(ref),
        "total_brands_with_alerts": len(alerts),
        "alerts": alerts,
    }


# ── Storage & Orchestration ──────────────────────────────────────────────────


def store_insight(db: Session, insight_type: str, data: dict):
    """Store computed insight, replacing any existing for same type+week."""
    week = data.get("week", _week_label())
    json_data = json.dumps(data, default=str)

    existing = db.query(InsightCache).filter(
        InsightCache.insight_type == insight_type,
        InsightCache.week_label == week,
    ).first()

    if existing:
        existing.data = json_data
        existing.computed_at = datetime.utcnow()
    else:
        entry = InsightCache(
            insight_type=insight_type,
            week_label=week,
            data=json_data,
            computed_at=datetime.utcnow(),
        )
        db.add(entry)
    db.commit()


def get_latest_insight(db: Session, insight_type: str) -> dict | None:
    """Return the most recently computed insight of the given type."""
    row = db.query(InsightCache).filter(
        InsightCache.insight_type == insight_type,
    ).order_by(InsightCache.computed_at.desc()).first()

    if not row:
        return None
    return json.loads(row.data)


COMPUTE_FUNCTIONS = {
    "weekly_scoreboard": compute_weekly_scoreboard,
    "brand_sequences": compute_brand_sequences,
    "subject_formulas": compute_subject_formulas,
    "new_tactics": compute_new_tactics,
}


def compute_and_store(db: Session, insight_type: str, reference_date: datetime = None) -> dict:
    """Compute one insight type and store it."""
    if insight_type not in COMPUTE_FUNCTIONS:
        raise ValueError(f"Unknown insight type: {insight_type}. Valid: {list(COMPUTE_FUNCTIONS.keys())}")
    data = COMPUTE_FUNCTIONS[insight_type](db, reference_date)
    store_insight(db, insight_type, data)
    return data


def compute_all(db: Session, reference_date: datetime = None) -> dict:
    """Compute and store all 4 insights. Returns summary."""
    results = {}
    for insight_type in COMPUTE_FUNCTIONS:
        try:
            data = compute_and_store(db, insight_type, reference_date)
            results[insight_type] = {"status": "ok", "week": data.get("week")}
        except Exception as e:
            results[insight_type] = {"status": "error", "error": str(e)}
    return results
