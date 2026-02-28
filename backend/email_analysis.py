"""
Rule-based email analysis engine.
Scores emails across 5 dimensions: Subject, Copy, CTA, Design, Strategy.
No AI calls — all deterministic, computed from email content.
"""

import re
from html.parser import HTMLParser
from datetime import datetime
from typing import Optional


# ── Grade calculation ──

def calculate_grade(score: int) -> str:
    if score >= 95: return "A+"
    if score >= 90: return "A"
    if score >= 85: return "A-"
    if score >= 80: return "B+"
    if score >= 70: return "B"
    if score >= 60: return "C"
    if score >= 50: return "D"
    return "F"


# ── HTML parsing helpers ──

class EmailHTMLParser(HTMLParser):
    """Extract structural elements from email HTML."""

    def __init__(self):
        super().__init__()
        self.links = []           # (href, text)
        self.images = []          # (src, alt)
        self.headings = []        # (tag, text)
        self.lists = 0            # count of <ul>/<ol>
        self.has_viewport = False
        self.has_dark_mode = False
        self.style_content = ""
        self.text_chunks = []
        self._current_tag = None
        self._current_attrs = {}
        self._current_text = ""
        self._in_style = False
        self._in_link = False
        self._link_href = ""
        self._link_text = ""

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)
        self._current_tag = tag

        if tag == "a":
            self._in_link = True
            self._link_href = attr_dict.get("href", "")
            self._link_text = ""

        elif tag == "img":
            self.images.append((
                attr_dict.get("src", ""),
                attr_dict.get("alt", ""),
            ))

        elif tag == "meta":
            name = attr_dict.get("name", "").lower()
            content = attr_dict.get("content", "").lower()
            if name == "viewport" or "viewport" in content:
                self.has_viewport = True

        elif tag == "style":
            self._in_style = True
            self.style_content = ""

        elif tag in ("ul", "ol"):
            self.lists += 1

        elif tag in ("h1", "h2", "h3", "h4"):
            self._current_tag = tag

    def handle_endtag(self, tag):
        if tag == "a" and self._in_link:
            self._in_link = False
            self.links.append((self._link_href, self._link_text.strip()))

        if tag == "style":
            self._in_style = False
            if "prefers-color-scheme" in self.style_content:
                self.has_dark_mode = True

        if tag in ("h1", "h2", "h3", "h4"):
            self.headings.append((tag, self._current_text.strip()))
            self._current_text = ""

        self._current_tag = None

    def handle_data(self, data):
        if self._in_style:
            self.style_content += data
        elif self._in_link:
            self._link_text += data
        else:
            self.text_chunks.append(data)

        if self._current_tag in ("h1", "h2", "h3", "h4"):
            self._current_text += data

    def get_body_text(self) -> str:
        return " ".join(self.text_chunks).strip()


def parse_html(html: str) -> EmailHTMLParser:
    parser = EmailHTMLParser()
    try:
        parser.feed(html or "")
    except Exception:
        pass
    return parser


# ── Power words & urgency terms ──

URGENCY_WORDS = {
    "now", "today", "hurry", "limited", "ending", "last chance",
    "final", "expires", "rush", "don't miss", "act now",
    "hours left", "ends tonight", "running out", "urgent",
    "deadline", "only", "exclusive", "flash",
}

POWER_WORDS = {
    "free", "new", "save", "exclusive", "guaranteed", "proven",
    "best", "top", "discover", "secret", "ultimate", "amazing",
    "bonus", "instant", "premium", "introducing", "special",
    "unlock", "win", "deal", "offer", "launch", "trending",
}

PERSONALIZATION_TOKENS = {
    "{first_name}", "{name}", "{firstname}", "{{first_name}}",
    "{{name}}", "%%first_name%%", "*|FNAME|*", "*|NAME|*",
}

CTA_ACTION_VERBS = {
    "shop", "buy", "get", "order", "grab", "claim", "start",
    "try", "explore", "discover", "download", "join", "sign up",
    "subscribe", "learn", "read", "view", "watch", "save",
    "book", "reserve", "add to cart", "checkout", "register",
}


# ── Dimension scorers ──

def score_subject(subject: str, email_type: Optional[str] = None) -> dict:
    findings = []
    score = 50  # start at 50, adjust up/down

    if not subject:
        return {"score": 0, "grade": "F", "findings": ["No subject line"]}

    # Length check (30-60 chars is ideal)
    length = len(subject)
    if 30 <= length <= 60:
        score += 15
        findings.append(f"Good length ({length} chars)")
    elif 20 <= length < 30:
        score += 8
        findings.append(f"Slightly short ({length} chars)")
    elif 60 < length <= 80:
        score += 5
        findings.append(f"Slightly long ({length} chars)")
    elif length > 80:
        score -= 10
        findings.append(f"Too long ({length} chars) — may get truncated")
    else:
        score -= 5
        findings.append(f"Very short ({length} chars)")

    subject_lower = subject.lower()

    # Personalization
    has_personalization = any(token.lower() in subject_lower for token in PERSONALIZATION_TOKENS)
    if has_personalization:
        score += 10
        findings.append("Has personalization token")
    else:
        findings.append("No personalization detected")

    # Urgency
    has_urgency = any(word in subject_lower for word in URGENCY_WORDS)
    if has_urgency:
        score += 8
        findings.append("Contains urgency words")

    # Power words
    power_found = [w for w in POWER_WORDS if w in subject_lower]
    if power_found:
        score += 5
        findings.append(f"Power words: {', '.join(power_found[:3])}")

    # Numbers (perform better in subject lines)
    if re.search(r'\d', subject):
        score += 5
        findings.append("Contains numbers (good for engagement)")

    # Emoji
    emoji_pattern = re.compile(
        "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF"
        "\U0001F680-\U0001F6FF\U0001F1E0-\U0001F1FF"
        "\U00002600-\U000026FF\U00002700-\U000027BF"
        "\U0001F900-\U0001F9FF\U0001FA00-\U0001FAFF]+", re.UNICODE
    )
    if emoji_pattern.search(subject):
        score += 3
        findings.append("Has emoji")

    # Question mark (drives curiosity)
    if "?" in subject:
        score += 3
        findings.append("Has question mark (drives curiosity)")

    # ALL CAPS spam check
    words = subject.split()
    caps_words = sum(1 for w in words if w.isupper() and len(w) > 2)
    if caps_words > len(words) * 0.5 and len(words) > 2:
        score -= 15
        findings.append("Excessive ALL CAPS — may trigger spam filters")
    elif caps_words > 0:
        score += 2
        findings.append(f"{caps_words} emphasized word(s)")

    # Clamp
    score = max(0, min(100, score))
    return {"score": score, "grade": calculate_grade(score), "findings": findings}


def score_copy(html: str, preview: Optional[str] = None) -> dict:
    findings = []
    score = 50

    parsed = parse_html(html)
    body_text = parsed.get_body_text()
    word_count = len(body_text.split()) if body_text else 0

    # Word count
    if 50 <= word_count <= 500:
        score += 15
        findings.append(f"Good word count ({word_count})")
    elif 500 < word_count <= 1000:
        score += 8
        findings.append(f"Lengthy copy ({word_count} words)")
    elif word_count > 1000:
        score += 2
        findings.append(f"Very long copy ({word_count} words) — consider trimming")
    elif 20 <= word_count < 50:
        score += 5
        findings.append(f"Short copy ({word_count} words)")
    else:
        score -= 5
        findings.append(f"Minimal copy ({word_count} words)")

    # Readability — sentence length
    sentences = re.split(r'[.!?]+', body_text)
    sentences = [s.strip() for s in sentences if s.strip()]
    if sentences:
        avg_sentence_len = sum(len(s.split()) for s in sentences) / len(sentences)
        if avg_sentence_len <= 20:
            score += 10
            findings.append(f"Good readability (avg {avg_sentence_len:.0f} words/sentence)")
        elif avg_sentence_len <= 30:
            score += 5
            findings.append(f"Moderate readability (avg {avg_sentence_len:.0f} words/sentence)")
        else:
            findings.append(f"Dense copy (avg {avg_sentence_len:.0f} words/sentence)")

    # Scannability — headers & lists
    if parsed.headings:
        score += 8
        findings.append(f"{len(parsed.headings)} heading(s) found — good structure")
    else:
        findings.append("No headings — consider adding structure")

    if parsed.lists > 0:
        score += 5
        findings.append(f"{parsed.lists} list(s) found — scannable")
    else:
        findings.append("No bullet/numbered lists found")

    # Preview text
    if preview and len(preview.strip()) > 10:
        score += 5
        findings.append("Has preview text")
    else:
        findings.append("No preview text detected")

    # Personalization in body
    body_lower = body_text.lower()
    has_personalization = any(token.lower() in (html or "").lower() for token in PERSONALIZATION_TOKENS)
    if has_personalization:
        score += 5
        findings.append("Body uses personalization")

    score = max(0, min(100, score))
    return {"score": score, "grade": calculate_grade(score), "findings": findings}


def score_cta(html: str) -> dict:
    findings = []
    score = 50

    parsed = parse_html(html)

    # Count CTA-like links (exclude unsubscribe, privacy, etc.)
    skip_patterns = {"unsubscribe", "privacy", "terms", "manage preferences", "view in browser"}
    cta_links = []
    for href, text in parsed.links:
        text_lower = text.lower().strip()
        if any(skip in text_lower for skip in skip_patterns):
            continue
        if text_lower and len(text_lower) > 1:
            cta_links.append((href, text))

    if not cta_links:
        score -= 20
        findings.append("No CTA links found")
        score = max(0, min(100, score))
        return {"score": score, "grade": calculate_grade(score), "findings": findings}

    findings.append(f"{len(cta_links)} CTA link(s) found")

    if 1 <= len(cta_links) <= 3:
        score += 15
        findings.append("Good number of CTAs")
    elif len(cta_links) <= 5:
        score += 10
    else:
        score += 3
        findings.append(f"Many CTAs ({len(cta_links)}) — may dilute focus")

    # Check for action verbs
    action_found = []
    for _, text in cta_links:
        text_lower = text.lower()
        for verb in CTA_ACTION_VERBS:
            if verb in text_lower:
                action_found.append(verb)
                break
    if action_found:
        score += 10
        findings.append(f"Action verbs: {', '.join(set(action_found)[:3])}")
    else:
        findings.append("No strong action verbs in CTAs")

    # Check if any link looks like a styled button (common patterns)
    html_lower = (html or "").lower()
    has_button_style = any(pattern in html_lower for pattern in [
        "background-color", "bgcolor", "btn", "button",
        "padding:", "border-radius",
    ])
    if has_button_style:
        score += 10
        findings.append("Styled button CTA detected")
    else:
        findings.append("Text-only CTAs — consider styled buttons")

    # Above-the-fold check: first CTA within first 30% of HTML
    if cta_links:
        first_cta_text = cta_links[0][1]
        first_pos = html.find(first_cta_text) if first_cta_text else -1
        html_len = len(html) if html else 1
        if 0 < first_pos < html_len * 0.3:
            score += 5
            findings.append("CTA placed above the fold")

    score = max(0, min(100, score))
    return {"score": score, "grade": calculate_grade(score), "findings": findings}


def score_design(html: str) -> dict:
    findings = []
    score = 50

    parsed = parse_html(html)

    # Images
    if parsed.images:
        score += 10
        findings.append(f"{len(parsed.images)} image(s) found")

        # Alt text check
        missing_alt = sum(1 for _, alt in parsed.images if not alt.strip())
        if missing_alt == 0:
            score += 8
            findings.append("All images have alt text")
        elif missing_alt < len(parsed.images):
            score += 3
            findings.append(f"{missing_alt} image(s) missing alt text")
        else:
            findings.append("No images have alt text — accessibility issue")

        # Image-to-text ratio
        body_text = parsed.get_body_text()
        word_count = len(body_text.split()) if body_text else 0
        if word_count > 0:
            ratio = len(parsed.images) / word_count * 100
            if ratio < 5:
                score += 5
                findings.append("Good image-to-text ratio")
            elif ratio > 20:
                findings.append("Heavy on images vs text — may affect deliverability")
    else:
        findings.append("No images — consider adding visuals")

    # Responsive check
    if parsed.has_viewport:
        score += 10
        findings.append("Has viewport meta tag (responsive)")
    else:
        findings.append("No viewport meta tag — may not render well on mobile")

    # Dark mode support
    if parsed.has_dark_mode:
        score += 8
        findings.append("Dark mode support detected")
    else:
        findings.append("No dark mode styles found")

    # Structured layout (tables or divs)
    html_lower = (html or "").lower()
    has_tables = "<table" in html_lower
    has_structure = has_tables or "display:flex" in html_lower or "display: flex" in html_lower
    if has_structure:
        score += 5
        findings.append("Structured layout detected")

    # Inline styles (common in email HTML)
    inline_style_count = html_lower.count('style="')
    if inline_style_count > 5:
        score += 3
        findings.append("Uses inline styles (email-safe)")

    score = max(0, min(100, score))
    return {"score": score, "grade": calculate_grade(score), "findings": findings}


def score_strategy(
    email_type: Optional[str],
    industry: Optional[str],
    received_at: Optional[datetime],
) -> dict:
    findings = []
    score = 50

    # Campaign type present
    if email_type:
        score += 15
        findings.append(f"Campaign type: {email_type}")
    else:
        findings.append("No campaign type identified")

    # Industry present
    if industry:
        score += 10
        findings.append(f"Industry: {industry}")
    else:
        findings.append("No industry classification")

    # Send timing
    if received_at:
        weekday = received_at.weekday()  # 0=Mon, 6=Sun
        hour = received_at.hour

        # Best days: Tue-Thu
        if weekday in (1, 2, 3):
            score += 10
            day_names = {1: "Tuesday", 2: "Wednesday", 3: "Thursday"}
            findings.append(f"Sent on {day_names[weekday]} (optimal)")
        elif weekday in (0, 4):
            score += 5
            day_names = {0: "Monday", 4: "Friday"}
            findings.append(f"Sent on {day_names[weekday]} (good)")
        else:
            day_names = {5: "Saturday", 6: "Sunday"}
            findings.append(f"Sent on {day_names.get(weekday, 'weekend')} (lower engagement typical)")

        # Best hours: 9-11 AM, 1-3 PM
        if 9 <= hour <= 11:
            score += 8
            findings.append(f"Sent at {hour}:00 (morning peak)")
        elif 13 <= hour <= 15:
            score += 6
            findings.append(f"Sent at {hour}:00 (afternoon peak)")
        elif 6 <= hour <= 20:
            score += 3
            findings.append(f"Sent at {hour}:00 (business hours)")
        else:
            findings.append(f"Sent at {hour}:00 (off-peak)")

    score = max(0, min(100, score))
    return {"score": score, "grade": calculate_grade(score), "findings": findings}


# ── Main entry point ──

DIMENSION_WEIGHTS = {
    "subject": 20,
    "copy": 25,
    "cta": 20,
    "design": 15,
    "strategy": 20,
}


def analyze_email(
    subject: str,
    html: str,
    email_type: Optional[str] = None,
    industry: Optional[str] = None,
    received_at: Optional[datetime] = None,
    preview: Optional[str] = None,
) -> dict:
    """
    Analyze an email and return scores across 5 dimensions.

    Returns dict with overall_score, overall_grade, and per-dimension breakdown.
    """
    dimensions = {
        "subject": score_subject(subject, email_type),
        "copy": score_copy(html, preview),
        "cta": score_cta(html),
        "design": score_design(html),
        "strategy": score_strategy(email_type, industry, received_at),
    }

    # Weighted overall score
    overall_score = 0
    for dim_name, dim_result in dimensions.items():
        weight = DIMENSION_WEIGHTS[dim_name]
        overall_score += dim_result["score"] * weight / 100

    overall_score = round(overall_score)

    return {
        "overall_score": overall_score,
        "overall_grade": calculate_grade(overall_score),
        "dimensions": dimensions,
    }
