from sqlalchemy import Column, String, Integer, Numeric
from sqlalchemy.types import JSON
from database import Base

class Product(Base):
    __tablename__ = "products"

    id = Column(String(36), primary_key=True, index=True)      # UUID
    name = Column(String(2000), nullable=False)                # <= 2000
    description = Column(String(10000), nullable=False, default="")  # <= 10000
    quantity = Column(Integer, nullable=False)                 # >= 0
    price = Column(Numeric(18, 2), nullable=False)             # > 0
    supplier_ids = Column(JSON, nullable=False, default=list)  # list[str]
    category_ids = Column(JSON, nullable=False, default=list)  # list[str]
    image_ids = Column(JSON, nullable=False, default=list)     # list[str]
