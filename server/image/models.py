from sqlalchemy import Column, String
from database import Base

# Each Image belongs to at most one Product (nullable product_id)
class Image(Base):
    __tablename__ = "images"

    id = Column(String(36), primary_key=True, index=True)    # UUID
    product_id = Column(String(36), nullable=True)           # UUID or null
    url = Column(String(2048), nullable=False)               # validated in schema
