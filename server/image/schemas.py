from typing import Optional, Annotated
from pydantic import BaseModel, Field, HttpUrl

class ImageBase(BaseModel):
    url: HttpUrl
    product_id: Optional[str] = None  # UUID or None

class ImageCreate(ImageBase):
    id: Optional[str] = None  # auto-generate if omitted

class ImageUpdate(BaseModel):
    url: Optional[HttpUrl] = None
    product_id: Optional[str] = Field(default=None)  # accept explicit null to detach

class ImageOut(ImageBase):
    id: str

    class Config:
        from_attributes = True
