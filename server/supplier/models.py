from sqlalchemy import Column, String
from sqlalchemy.types import JSON
from database import Base

# Supplier table per spec. product_ids are stored as JSON array of UUID strings.
class Supplier(Base):
    __tablename__ = "suppliers"

    id = Column(String(36), primary_key=True, index=True)      # UUID
    name = Column(String(2000), nullable=False)                # <= 2000
    contact = Column(String(320), nullable=False)              # Email (validated in schema)
    product_ids = Column(JSON, nullable=False, default=list)   # list[str]
