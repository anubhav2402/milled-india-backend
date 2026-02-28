"""
AI Email Generator — extracts template schema from existing emails
and generates new email content using Claude.
"""

import os
import re
import json
from html.parser import HTMLParser
from typing import Optional


# ── Template schema extraction ──

class TemplateExtractor(HTMLParser):
    """Parse email HTML and extract structural blocks + content slots."""

    def __init__(self):
        super().__init__()
        self.blocks = []
        self._current_block = None
        self._text_buffer = ""
        self._in_style = False
        self._styles = []
        self._link_stack = []
        self._img_count = 0
        self._cta_count = 0

    def handle_starttag(self, tag, attrs):
        attr_dict = dict(attrs)

        if tag == "style":
            self._in_style = True
            return

        if tag == "img":
            self._img_count += 1
            self.blocks.append({
                "type": "image",
                "slot": f"image_{self._img_count}",
                "original_src": attr_dict.get("src", ""),
                "alt": attr_dict.get("alt", ""),
                "width": attr_dict.get("width", ""),
            })

        if tag == "a":
            href = attr_dict.get("href", "")
            style = attr_dict.get("style", "")
            # Detect styled CTA buttons
            is_button = any(kw in style.lower() for kw in [
                "background-color", "background:", "padding:", "border-radius",
            ]) or any(kw in (attr_dict.get("class", "")).lower() for kw in [
                "btn", "button", "cta",
            ])
            if is_button:
                self._cta_count += 1
                self._link_stack.append(("cta", self._cta_count, href))
            else:
                self._link_stack.append(("link", 0, href))

        if tag in ("h1", "h2", "h3"):
            self._current_block = {"type": "heading", "tag": tag, "text": ""}

        if tag in ("p", "td", "div") and not self._current_block:
            self._current_block = {"type": "text", "tag": tag, "text": ""}

    def handle_endtag(self, tag):
        if tag == "style":
            self._in_style = False

        if tag in ("h1", "h2", "h3") and self._current_block and self._current_block["type"] == "heading":
            text = self._current_block["text"].strip()
            if text and len(text) > 2:
                self.blocks.append({
                    "type": "heading",
                    "tag": self._current_block["tag"],
                    "slot": f"heading_{len([b for b in self.blocks if b['type'] == 'heading']) + 1}",
                    "original_text": text,
                })
            self._current_block = None

        if tag == "a" and self._link_stack:
            link_info = self._link_stack.pop()
            if link_info[0] == "cta":
                text = self._text_buffer.strip()
                self.blocks.append({
                    "type": "cta",
                    "slot": f"cta_{link_info[1]}",
                    "original_text": text if text else "Shop Now",
                    "original_url": link_info[2],
                })
                self._text_buffer = ""

        if tag in ("p", "td", "div") and self._current_block and self._current_block["type"] == "text":
            text = self._current_block["text"].strip()
            if text and len(text) > 10:
                self.blocks.append({
                    "type": "text",
                    "slot": f"body_{len([b for b in self.blocks if b['type'] == 'text']) + 1}",
                    "original_text": text[:200],
                })
            self._current_block = None

    def handle_data(self, data):
        if self._in_style:
            self._styles.append(data)
            return
        clean = data.strip()
        if self._current_block:
            self._current_block["text"] += " " + clean
        self._text_buffer = clean


def extract_template_schema(html: str) -> dict:
    """
    Extract a template schema from email HTML.
    Returns structured blocks that can be used for AI generation.
    """
    extractor = TemplateExtractor()
    try:
        extractor.feed(html or "")
    except Exception:
        pass

    # Deduplicate and limit
    blocks = extractor.blocks[:20]

    # Identify template structure
    has_hero = any(b["type"] == "image" and b.get("slot") == "image_1" for b in blocks)
    heading_count = len([b for b in blocks if b["type"] == "heading"])
    cta_count = len([b for b in blocks if b["type"] == "cta"])
    body_count = len([b for b in blocks if b["type"] == "text"])

    return {
        "blocks": blocks,
        "structure": {
            "has_hero_image": has_hero,
            "heading_count": heading_count,
            "cta_count": max(cta_count, 1),
            "body_sections": body_count,
        },
        "slots": [b["slot"] for b in blocks if "slot" in b],
    }


# ── Claude AI generation ──

def _get_anthropic_client():
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable not set")
    from anthropic import Anthropic
    return Anthropic(api_key=api_key)


SYSTEM_PROMPT = """You are an expert email copywriter and HTML developer. You create professional marketing emails.

Given a template structure and brand details, generate:
1. All text content (headings, body copy, CTA text) that matches the brand's voice
2. A complete, production-ready HTML email

Rules:
- Use inline CSS only (no external stylesheets)
- Use tables for layout (email-safe)
- Keep the same structural layout as the template (same number of sections, CTAs, images)
- Make the copy compelling, on-brand, and action-oriented
- Include a meta viewport tag for mobile responsiveness
- Use the brand's color scheme if provided, otherwise use professional defaults
- Replace placeholder image URLs with descriptive alt text
- Keep subject line under 60 characters
- The response must be valid JSON with the exact schema specified"""


def generate_email(
    template_schema: dict,
    brand_name: str,
    brand_url: str = "",
    industry: str = "",
    tone: str = "professional",
    instructions: str = "",
) -> dict:
    """
    Generate a new email based on a template schema and brand details.

    Returns dict with: subject, preview_text, html, slots (filled content)
    """
    client = _get_anthropic_client()

    user_prompt = f"""Generate a marketing email for this brand:

Brand: {brand_name}
Website: {brand_url}
Industry: {industry}
Tone: {tone}
{f'Special instructions: {instructions}' if instructions else ''}

Based on this template structure:
{json.dumps(template_schema, indent=2)}

Return a JSON object with this exact structure:
{{
  "subject": "Subject line (under 60 chars)",
  "preview_text": "Preview text shown in inbox (under 100 chars)",
  "html": "Complete HTML email code with inline styles, table layout, viewport meta tag",
  "slots": {{
    "heading_1": "Generated heading text",
    "body_1": "Generated body text",
    "cta_1": "Generated CTA text"
  }}
}}

Important: The "html" field must contain a COMPLETE, production-ready HTML email that renders well in Gmail, Outlook, and Apple Mail. Use tables for layout. All CSS must be inline."""

    response = client.messages.create(
        model="claude-sonnet-4-5-20241022",
        max_tokens=4096,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_prompt}],
        timeout=60,
    )

    result_text = response.content[0].text.strip()

    # Strip markdown code fences if present
    if result_text.startswith("```"):
        lines = result_text.split("\n")
        # Remove first and last lines if they're fences
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        result_text = "\n".join(lines)

    try:
        result = json.loads(result_text)
    except json.JSONDecodeError:
        # Try to extract JSON from the response
        json_match = re.search(r'\{[\s\S]*\}', result_text)
        if json_match:
            try:
                result = json.loads(json_match.group())
            except json.JSONDecodeError:
                raise ValueError("Failed to parse AI response as JSON")
        else:
            raise ValueError("AI response did not contain valid JSON")

    # Validate required fields
    if "html" not in result:
        raise ValueError("AI response missing 'html' field")

    return {
        "subject": result.get("subject", ""),
        "preview_text": result.get("preview_text", ""),
        "html": result["html"],
        "slots": result.get("slots", {}),
    }
