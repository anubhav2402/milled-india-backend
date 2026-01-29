"""Utility functions for email processing."""
from bs4 import BeautifulSoup
from typing import Optional


def extract_preview_image_url(html: str) -> Optional[str]:
    """
    Extract the first image URL from email HTML.
    Returns None if no image is found.
    """
    if not html:
        return None
    
    try:
        soup = BeautifulSoup(html, "html.parser")
        # Find the first img tag
        img_tag = soup.find("img")
        if img_tag and img_tag.get("src"):
            src = img_tag.get("src")
            # Handle relative URLs (some emails use relative paths)
            # For now, return as-is. In production, you might want to convert
            # relative URLs to absolute URLs based on the email's domain
            return src
    except Exception:
        # If parsing fails, return None
        pass
    
    return None
