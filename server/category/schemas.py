from pydantic import BaseModel, Field, constr
from typing import List, Optional

NameStr = constr(min_length=1, max_length=2000)
DescStr = constr(min_length=0, max_length=10000)

class CategoryBase(BaseModel):
    name: NameStr
    description: DescStr = ""
    product_ids: List[str] = Field(default_factory=list)

class CategoryCreate(CategoryBase):
    id: Optional[str] = None  # auto-generate UUID if omitted

class CategoryUpdate(BaseModel):
    name: Optional[NameStr] = None
    description: Optional[DescStr] = None
    product_ids: Optional[List[str]] = None

class LinkProductOp(BaseModel):
    product_id: str

class CategoryOut(CategoryBase):
    id: str

    class Config:
        from_attributes = True
