from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import List, Optional
import uuid

from models import Category
from schemas import CategoryCreate, CategoryUpdate

def _validate_uuid(id_str: str) -> None:
    try:
        uuid.UUID(str(id_str))
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid UUID: {id_str}")

def _clean_ids(ids: Optional[List[str]]) -> Optional[List[str]]:
    if ids is None:
        return None
    seen = set()
    cleaned: List[str] = []
    for pid in ids:
        _validate_uuid(pid)
        if pid not in seen:
            seen.add(pid)
            cleaned.append(pid)
    return cleaned

def get(db: Session, category_id: str) -> Category:
    cat = db.query(Category).filter(Category.id == category_id).first()
    if not cat:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Category not found")
    return cat

def list_all(db: Session, skip: int = 0, limit: int = 100) -> List[Category]:
    return db.query(Category).offset(skip).limit(limit).all()

def create(db: Session, payload: CategoryCreate) -> Category:
    cat_id = payload.id or str(uuid.uuid4())
    _validate_uuid(cat_id)
    product_ids = _clean_ids(payload.product_ids) or []
    obj = Category(
        id=cat_id,
        name=payload.name,
        description=payload.description or "",
        product_ids=product_ids,
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def update(db: Session, category_id: str, payload: CategoryUpdate) -> Category:
    cat = get(db, category_id)
    if payload.name is not None:
        cat.name = payload.name
    if payload.description is not None:
        cat.description = payload.description
    if payload.product_ids is not None:
        cat.product_ids = _clean_ids(payload.product_ids) or []
    db.commit()
    db.refresh(cat)
    return cat

def delete(db: Session, category_id: str) -> None:
    cat = get(db, category_id)
    db.delete(cat)
    db.commit()

def add_product(db: Session, category_id: str, product_id: str) -> Category:
    _validate_uuid(product_id)
    cat = get(db, category_id)
    if product_id not in cat.product_ids:
        cat.product_ids.append(product_id)
        db.commit()
        db.refresh(cat)
    return cat

def remove_product(db: Session, category_id: str, product_id: str) -> Category:
    _validate_uuid(product_id)
    cat = get(db, category_id)
    if product_id in cat.product_ids:
        cat.product_ids = [x for x in cat.product_ids if x != product_id]
        db.commit()
        db.refresh(cat)
    return cat
