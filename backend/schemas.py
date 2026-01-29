from datetime import datetime
from typing import Optional, List

from pydantic import BaseModel, EmailStr


# ============ Auth Schemas ============

class UserCreate(BaseModel):
    email: EmailStr
    password: str
    name: Optional[str] = None


class UserLogin(BaseModel):
    email: EmailStr
    password: str


class GoogleAuth(BaseModel):
    token: str  # Google ID token from frontend


class UserOut(BaseModel):
    id: int
    email: str
    name: Optional[str] = None

    class Config:
        from_attributes = True


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    user: UserOut


class FollowedBrand(BaseModel):
    brand_name: str
    created_at: datetime

    class Config:
        from_attributes = True


class UserFollowsResponse(BaseModel):
    follows: List[str]  # List of brand names


# ============ Email Schemas ============

class EmailBase(BaseModel):
    gmail_id: str
    subject: str
    sender: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    type: Optional[str] = None
    industry: Optional[str] = None
    received_at: datetime
    preview: Optional[str] = None


class EmailCreate(EmailBase):
    html: str


class EmailOut(EmailBase):
    id: int
    html: str
    preview_image_url: Optional[str] = None

    class Config:
        from_attributes = True


class EmailListOut(EmailBase):
    """Lightweight schema for email listing (no HTML for faster loading)."""
    id: int
    preview_image_url: Optional[str] = None

    class Config:
        from_attributes = True

