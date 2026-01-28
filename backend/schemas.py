from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class EmailBase(BaseModel):
    gmail_id: str
    subject: str
    sender: Optional[str] = None
    brand: Optional[str] = None
    category: Optional[str] = None
    type: Optional[str] = None
    received_at: datetime
    preview: Optional[str] = None


class EmailCreate(EmailBase):
    html: str


class EmailOut(EmailBase):
    id: int

    class Config:
        orm_mode = True

