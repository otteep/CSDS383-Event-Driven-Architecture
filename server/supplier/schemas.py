from typing import List, Optional, Annotated
from pydantic import BaseModel, EmailStr, Field

NameStr = Annotated[str, Field(min_length=1, max_length=2000)]

class SupplierBase(BaseModel):
    name: NameStr
    contact: EmailStr
    product_ids: List[str] = Field(default_factory=list)

class SupplierCreate(SupplierBase):
    id: Optional[str] = None  # UUID auto-gen if omitted

class SupplierUpdate(BaseModel):
    name: Optional[NameStr] = None
    contact: Optional[EmailStr] = None
    product_ids: Optional[List[str]] = None

class LinkProductOp(BaseModel):
    product_id: str

class SupplierOut(SupplierBase):
    id: str

    class Config:
        from_attributes = True
