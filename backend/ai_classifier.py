"""
AI-powered classification for brand industries and campaign types.
Uses OpenAI GPT-4o-mini for accurate classification with caching.
"""

import os
import json
from typing import Optional, Dict, Any

# Industry categories (must match engine.py)
INDUSTRIES = [
    "Beauty & Personal Care",
    "Women's Fashion",
    "Men's Fashion",
    "Food & Beverages",
    "Travel & Hospitality",
    "Electronics & Gadgets",
    "Home & Living",
    "Health & Wellness",
    "Finance & Fintech",
    "Kids & Baby",
    "Sports & Fitness",
    "Entertainment",
    "General Retail",
]

# Campaign types (must match engine.py)
CAMPAIGN_TYPES = [
    "Sale",
    "Welcome",
    "Abandoned Cart",
    "Newsletter",
    "New Arrival",
    "Re-engagement",
    "Order Update",
    "Festive",
    "Loyalty",
    "Feedback",
]


def get_openai_client():
    """Get OpenAI client with API key from environment."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable not set")
    
    from openai import OpenAI
    return OpenAI(api_key=api_key)


def classify_brand_with_ai(
    brand_name: str,
    subject: Optional[str] = None,
    preview: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Use OpenAI to classify a brand's industry and campaign type.
    
    Args:
        brand_name: The brand name to classify
        subject: Email subject line (optional, helps with context)
        preview: Email preview text (optional, helps with context)
    
    Returns:
        Dict with keys: industry, campaign_type, confidence
    """
    client = get_openai_client()
    
    # Build context from available information
    context_parts = [f"Brand: {brand_name}"]
    if subject:
        context_parts.append(f"Email Subject: {subject}")
    if preview:
        context_parts.append(f"Email Preview: {preview[:500]}")
    
    context = "\n".join(context_parts)
    
    # Create the classification prompt
    prompt = f"""You are a marketing email classifier. Classify the following brand into an industry category.

{context}

IMPORTANT: You must select from ONLY these industry categories:
{json.dumps(INDUSTRIES, indent=2)}

If the brand sells multiple product types (like Amazon, Flipkart), classify as "General Retail".
For luxury fashion brands (Gucci, Zara, H&M), classify based on their primary focus:
- If primarily women's clothing: "Women's Fashion"
- If primarily men's clothing: "Men's Fashion"
- If mixed equally: "Women's Fashion" (default for fashion brands)

Also classify the campaign type from ONLY these options:
{json.dumps(CAMPAIGN_TYPES, indent=2)}

If unsure about campaign type, use "Newsletter" as default.

Respond with ONLY a JSON object in this exact format:
{{"industry": "category name", "campaign_type": "type name", "confidence": 0.95}}

The confidence should be between 0.5 and 1.0 based on how certain you are.
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise classifier. Always respond with valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,  # Low temperature for consistent results
            max_tokens=100,
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Parse JSON response
        # Handle potential markdown code blocks
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.startswith("json"):
                result_text = result_text[4:]
            result_text = result_text.strip()
        
        result = json.loads(result_text)
        
        # Validate industry is in allowed list
        if result.get("industry") not in INDUSTRIES:
            print(f"AI returned invalid industry '{result.get('industry')}', defaulting to General Retail")
            result["industry"] = "General Retail"
        
        # Validate campaign type is in allowed list
        if result.get("campaign_type") not in CAMPAIGN_TYPES:
            result["campaign_type"] = "Newsletter"
        
        # Ensure confidence is within bounds
        confidence = result.get("confidence", 0.8)
        if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
            confidence = 0.8
        result["confidence"] = confidence
        
        return result
        
    except json.JSONDecodeError as e:
        print(f"Failed to parse AI response as JSON: {e}")
        print(f"Raw response was: {result_text}")
        return {
            "industry": "General Retail",
            "campaign_type": "Newsletter",
            "confidence": 0.5,
            "error": f"JSON parse error: {e}"
        }
    except Exception as e:
        import traceback
        print(f"AI classification error: {e}")
        print(traceback.format_exc())
        return {
            "industry": "General Retail",
            "campaign_type": "Newsletter", 
            "confidence": 0.5,
            "error": str(e)
        }


def classify_campaign_type_with_ai(
    subject: str,
    preview: Optional[str] = None,
    brand_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Use OpenAI to classify just the campaign type of an email.
    
    Args:
        subject: Email subject line
        preview: Email preview text (optional)
        brand_name: Brand name (optional, helps with context)
    
    Returns:
        Dict with keys: campaign_type, confidence
    """
    client = get_openai_client()
    
    # Build context
    context_parts = [f"Subject: {subject}"]
    if preview:
        context_parts.append(f"Preview: {preview[:300]}")
    if brand_name:
        context_parts.append(f"Brand: {brand_name}")
    
    context = "\n".join(context_parts)
    
    prompt = f"""Classify this marketing email into a campaign type.

{context}

Choose ONLY from these campaign types:
{json.dumps(CAMPAIGN_TYPES, indent=2)}

Guidelines:
- "Sale" = discounts, offers, % off, deals, clearance
- "Welcome" = first email after signup, onboarding
- "Abandoned Cart" = reminders about items left in cart
- "Newsletter" = regular updates, news, content
- "New Arrival" = new products, launches, collections
- "Re-engagement" = win-back emails, "we miss you"
- "Order Update" = shipping, delivery, tracking
- "Festive" = holiday-themed (Diwali, Christmas, etc.)
- "Loyalty" = points, rewards, member benefits
- "Feedback" = reviews, surveys, ratings

Respond with ONLY a JSON object:
{{"campaign_type": "type name", "confidence": 0.95}}
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You are a precise classifier. Always respond with valid JSON only."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.1,
            max_tokens=50,
        )
        
        result_text = response.choices[0].message.content.strip()
        
        # Handle markdown code blocks
        if result_text.startswith("```"):
            result_text = result_text.split("```")[1]
            if result_text.startswith("json"):
                result_text = result_text[4:]
            result_text = result_text.strip()
        
        result = json.loads(result_text)
        
        # Validate campaign type
        if result.get("campaign_type") not in CAMPAIGN_TYPES:
            result["campaign_type"] = "Newsletter"
        
        # Ensure confidence is valid
        confidence = result.get("confidence", 0.8)
        if not isinstance(confidence, (int, float)) or confidence < 0 or confidence > 1:
            confidence = 0.8
        result["confidence"] = confidence
        
        return result
        
    except Exception as e:
        print(f"AI campaign classification error: {e}")
        return {
            "campaign_type": "Newsletter",
            "confidence": 0.5
        }


def is_ai_available() -> bool:
    """Check if OpenAI API is configured and available."""
    return bool(os.getenv("OPENAI_API_KEY"))
