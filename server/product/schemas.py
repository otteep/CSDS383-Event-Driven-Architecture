from typing import List, Optional, Annotated
from pydantic import BaseModel, Field
from decimal import Decimal

NameStr = Annotated[str, Field(min_length=1, max_length=2000)]
DescStr = Annotated[str, Field(min_length=0, max_length=10000)]

class ProductBase(BaseModel):
    name: NameStr
    description: DescStr = ""
    quantity: Annotated[int, Field(ge=0)]
    price: Annotated[Decimal, Field(gt=0)]
    supplier_ids: List[str] = Field(default_factory=list)
    category_ids: List[str] = Field(default_factory=list)
    image_ids: List[str] = Field(default_factory=list)

class ProductCreate(ProductBase):
    id: Optional[str] = None  # UUID auto-generated if omitted

class ProductUpdate(BaseModel):
    name: Optional[NameStr] = None
    description: Optional[DescStr] = None
    quantity: Optional[Annotated[int, Field(ge=0)]] = None
    price: Optional[Annotated[Decimal, Field(gt=0)]] = None
    supplier_ids: Optional[List[str]] = None
    category_ids: Optional[List[str]] = None
    image_ids: Optional[List[str]] = None

class ProductOut(ProductBase):
    id: str

    class Config:
        from_attributes = True
