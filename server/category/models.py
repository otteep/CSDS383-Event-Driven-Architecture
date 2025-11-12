from sqlalchemy import Column, String
from sqlalchemy.types import JSON
from database import Base

# Category per spec: id, name<=2000, description<=10000, product_ids list
class Category(Base):
    __tablename__ = "categories"

    id = Column(String(36), primary_key=True, index=True)        # UUID
    name = Column(String(2000), nullable=False)
    description = Column(String(10000), nullable=False, default="")
    product_ids = Column(JSON, nullable=False, default=list)     # list[str] of product UUIDs
