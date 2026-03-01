"""
AI-powered classification for brand industries and campaign types.
Uses OpenAI GPT-4o-mini for accurate classification with caching.
"""

import os
import json
from typing import Optional, Dict, Any

# Industry categories (must match engine.py)
INDUSTRIES = [
    "Apparel & Accessories",
    "Baby & Kids",
    "Beauty & Personal Care",
    "Books, Art & Stationery",
    "Business & B2B Retail",
    "Electronics & Tech",
    "Entertainment",
    "Finance & Fintech",
    "Food & Beverage",
    "General / Department Store",
    "Gifts & Lifestyle",
    "Health, Fitness & Wellness",
    "Home & Living",
    "Luxury & High-End Goods",
    "Pets",
    "Tools, Auto & DIY",
    "Travel & Outdoors",
]

# Subcategories per main industry
SUBCATEGORIES = {
    "Apparel & Accessories": [
        "Activewear / Athleisure", "Bags & Handbags", "Footwear",
        "Hats & Accessories", "Intimates / Lingerie", "Jewelry",
        "Men's Clothing", "Outerwear", "Sunglasses & Eyewear",
        "Swimwear", "Unisex / Gender-Neutral Clothing", "Watches",
        "Women's Clothing", "Others",
    ],
    "Baby & Kids": [
        "Baby Gear", "Clothing", "Diapers & Hygiene", "Educational Products",
        "Feeding & Nursing", "Kids' Furniture", "Toys & Games", "Others",
    ],
    "Beauty & Personal Care": [
        "Bath & Body", "Beauty Tools & Devices", "Clean / Organic Beauty",
        "Fragrance / Perfume", "Grooming / Shaving", "Haircare",
        "Makeup / Cosmetics", "Oral Care", "Skincare", "Others",
    ],
    "Books, Art & Stationery": [
        "Art Supplies", "Crafting & DIY Kits", "Educational / Academic",
        "Fiction / Non-Fiction", "Journals & Planners",
        "Notebooks / Writing Tools", "Others",
    ],
    "Business & B2B Retail": [
        "Corporate Gifts", "Office Supplies", "Packaging & Fulfillment",
        "Promotional Products", "Others",
    ],
    "Electronics & Tech": [
        "Cameras & Photography", "Computers & Laptops", "Drones & Gadgets",
        "Gaming Consoles & Accessories", "Headphones & Audio Gear",
        "Smart Home Devices", "Smartphones", "Smartwatches & Wearables",
        "Tablets & Accessories", "Others",
    ],
    "Entertainment": [
        "Streaming", "Events & Ticketing", "Music", "Gaming", "Others",
    ],
    "Finance & Fintech": [
        "Payments", "Banking", "Insurance", "Investment", "Credit Cards", "Others",
    ],
    "Food & Beverage": [
        "Alcohol", "Beverages (Coffee, Tea, Juices)", "Cooking Ingredients & Spices",
        "Meal Kits", "Pantry Staples", "Snacks & Treats", "Specialty Foods",
        "Subscription Boxes", "Others",
    ],
    "General / Department Store": [
        "Multi-Category Retail", "Online Marketplaces", "Flash Sale Retailers", "Others",
    ],
    "Gifts & Lifestyle": [
        "Eco-Friendly / Sustainable Products", "Gift Cards",
        "Hobby & Craft Supplies", "Novelty & Fun Items", "Personalized Gifts",
        "Seasonal / Holiday Gifts", "Subscription Boxes", "Others",
    ],
    "Health, Fitness & Wellness": [
        "Fitness Equipment", "Mental Health / Meditation",
        "Personal Health Devices", "Supplements", "Vitamins & Nutrition",
        "Wearable Fitness Trackers", "Yoga & Recovery Gear", "Others",
    ],
    "Home & Living": [
        "Bedding & Bath", "Cleaning Supplies", "Furniture", "Home Décor",
        "Home Improvement", "Kitchen & Dining", "Lawn & Garden", "Lighting",
        "Rugs & Curtains", "Smart Home Devices", "Storage & Organization", "Others",
    ],
    "Luxury & High-End Goods": [
        "Collectibles & Limited Editions", "Designer Fashion", "Fine Jewelry",
        "Premium Skincare", "Others",
    ],
    "Pets": [
        "Pet Food", "Pet Apparel", "Pet Grooming", "Pet Health / Supplements",
        "Pet Toys", "Accessories", "Beds & Crates", "Others",
    ],
    "Tools, Auto & DIY": [
        "Automotive Accessories", "Car Cleaning & Care", "Hand Tools",
        "Hardware Supplies", "Home DIY Kits", "Lawn & Garden", "Power Tools", "Others",
    ],
    "Travel & Outdoors": [
        "Camping & Hiking Gear", "Coolers / Hydration",
        "Luggage & Travel Accessories", "Outdoor Furniture",
        "Travel Skincare & Essentials", "Beachwear & Travel Apparel", "Others",
    ],
}

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
    
    # Build subcategory reference for the prompt
    subcategory_ref = "\n".join(
        f"  {ind}: {', '.join(subs)}"
        for ind, subs in SUBCATEGORIES.items()
    )

    # Create the classification prompt
    prompt = f"""You are a marketing email classifier. Classify the following brand into an industry category and subcategory.

{context}

IMPORTANT: You must select from ONLY these industry categories:
{json.dumps(INDUSTRIES, indent=2)}

And for each industry, pick a subcategory from this list:
{subcategory_ref}

Guidelines:
- Multi-category retailers (Amazon, Flipkart, Meesho) → "General / Department Store"
- Athletic/sportswear brands (Nike, Puma, Adidas) → "Apparel & Accessories" with subcategory "Activewear / Athleisure"
- Luxury brands (Gucci, Balenciaga, Louis Vuitton) → "Luxury & High-End Goods" with subcategory "Designer Fashion"
- If the brand primarily sells women's clothing → "Apparel & Accessories" / "Women's Clothing"
- If the brand primarily sells men's clothing → "Apparel & Accessories" / "Men's Clothing"
- Jewelry brands → "Apparel & Accessories" / "Jewelry"
- If unsure about subcategory, use "Others"

Also classify the campaign type from ONLY these options:
{json.dumps(CAMPAIGN_TYPES, indent=2)}

If unsure about campaign type, use "Newsletter" as default.

Respond with ONLY a JSON object in this exact format:
{{"industry": "category name", "subcategory": "subcategory name", "campaign_type": "type name", "confidence": 0.95}}

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
            print(f"AI returned invalid industry '{result.get('industry')}', defaulting to General / Department Store")
            result["industry"] = "General / Department Store"

        # Validate subcategory is in allowed list for that industry
        valid_subs = SUBCATEGORIES.get(result["industry"], [])
        if result.get("subcategory") not in valid_subs:
            result["subcategory"] = "Others"

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
            "industry": "General / Department Store",
            "subcategory": "Others",
            "campaign_type": "Newsletter",
            "confidence": 0.5,
            "error": f"JSON parse error: {e}"
        }
    except Exception as e:
        import traceback
        print(f"AI classification error: {e}")
        print(traceback.format_exc())
        return {
            "industry": "General / Department Store",
            "subcategory": "Others",
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
