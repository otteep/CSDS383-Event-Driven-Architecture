from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import List, Optional
from decimal import Decimal
import uuid

from models import Product
from schemas import ProductCreate, ProductUpdate

def _validate_uuid(id_str: str) -> None:
    try:
        uuid.UUID(str(id_str))
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid UUID: {id_str}")

def _clean_ids(ids: Optional[List[str]]) -> Optional[List[str]]:
    if ids is None:
        return None
    out, seen = [], set()
    for s in ids:
        _validate_uuid(s)
        if s not in seen:
            seen.add(s)
            out.append(s)
    return out

def get(db: Session, product_id: str) -> Product:
    obj = db.query(Product).filter(Product.id == product_id).first()
    if not obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Product not found")
    return obj

def list_all(db: Session, skip: int = 0, limit: int = 100) -> List[Product]:
    return db.query(Product).offset(skip).limit(limit).all()

def create(db: Session, payload: ProductCreate) -> Product:
    pid = payload.id or str(uuid.uuid4())
    _validate_uuid(pid)

    obj = Product(
        id=pid,
        name=payload.name,
        description=payload.description or "",
        quantity=int(payload.quantity),
        price=Decimal(payload.price),
        supplier_ids=_clean_ids(payload.supplier_ids) or [],
        category_ids=_clean_ids(payload.category_ids) or [],
        image_ids=_clean_ids(payload.image_ids) or [],
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def update(db: Session, product_id: str, payload: ProductUpdate) -> Product:
    obj = get(db, product_id)

    if payload.name is not None: obj.name = payload.name
    if payload.description is not None: obj.description = payload.description
    if payload.quantity is not None:
        if payload.quantity < 0:
            raise HTTPException(status_code=422, detail="quantity must be >= 0")
        obj.quantity = int(payload.quantity)
    if payload.price is not None:
        if payload.price <= 0:
            raise HTTPException(status_code=422, detail="price must be > 0")
        obj.price = Decimal(payload.price)

    if payload.supplier_ids is not None:
        obj.supplier_ids = _clean_ids(payload.supplier_ids) or []
    if payload.category_ids is not None:
        obj.category_ids = _clean_ids(payload.category_ids) or []
    if payload.image_ids is not None:
        obj.image_ids = _clean_ids(payload.image_ids) or []

    db.commit()
    db.refresh(obj)
    return obj

def delete(db: Session, product_id: str) -> None:
    obj = get(db, product_id)
    db.delete(obj)
    db.commit()

# Link/unlink helpers (used by relationship endpoints)
def add_id(ids: List[str], new_id: str) -> List[str]:
    return ids if new_id in ids else (ids + [new_id])

def remove_id(ids: List[str], drop_id: str) -> List[str]:
    return [x for x in ids if x != drop_id]

def add_supplier(db: Session, product_id: str, supplier_id: str) -> Product:
    _validate_uuid(supplier_id)
    obj = get(db, product_id)
    obj.supplier_ids = add_id(obj.supplier_ids or [], supplier_id)
    db.commit(); db.refresh(obj); return obj

def remove_supplier(db: Session, product_id: str, supplier_id: str) -> Product:
    _validate_uuid(supplier_id)
    obj = get(db, product_id)
    obj.supplier_ids = remove_id(obj.supplier_ids or [], supplier_id)
    db.commit(); db.refresh(obj); return obj

def add_category(db: Session, product_id: str, category_id: str) -> Product:
    _validate_uuid(category_id)
    obj = get(db, product_id)
    obj.category_ids = add_id(obj.category_ids or [], category_id)
    db.commit(); db.refresh(obj); return obj

def remove_category(db: Session, product_id: str, category_id: str) -> Product:
    _validate_uuid(category_id)
    obj = get(db, product_id)
    obj.category_ids = remove_id(obj.category_ids or [], category_id)
    db.commit(); db.refresh(obj); return obj

def add_image(db: Session, product_id: str, image_id: str) -> Product:
    _validate_uuid(image_id)
    obj = get(db, product_id)
    obj.image_ids = add_id(obj.image_ids or [], image_id)
    db.commit(); db.refresh(obj); return obj

def remove_image(db: Session, product_id: str, image_id: str) -> Product:
    _validate_uuid(image_id)
    obj = get(db, product_id)
    obj.image_ids = remove_id(obj.image_ids or [], image_id)
    db.commit(); db.refresh(obj); return obj
