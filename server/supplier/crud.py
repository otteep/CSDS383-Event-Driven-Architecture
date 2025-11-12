from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import List, Optional
import uuid

from models import Supplier
from schemas import SupplierCreate, SupplierUpdate

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

def get(db: Session, supplier_id: str) -> Supplier:
    obj = db.query(Supplier).filter(Supplier.id == supplier_id).first()
    if not obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Supplier not found")
    return obj

def list_all(db: Session, skip: int = 0, limit: int = 100) -> List[Supplier]:
    return db.query(Supplier).offset(skip).limit(limit).all()

def create(db: Session, payload: SupplierCreate) -> Supplier:
    sid = payload.id or str(uuid.uuid4())
    _validate_uuid(sid)
    obj = Supplier(
        id=sid,
        name=payload.name,
        contact=str(payload.contact),
        product_ids=_clean_ids(payload.product_ids) or [],
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def update(db: Session, supplier_id: str, payload: SupplierUpdate) -> Supplier:
    obj = get(db, supplier_id)
    if payload.name is not None: obj.name = payload.name
    if payload.contact is not None: obj.contact = str(payload.contact)
    if payload.product_ids is not None: obj.product_ids = _clean_ids(payload.product_ids) or []
    db.commit(); db.refresh(obj)
    return obj

def delete(db: Session, supplier_id: str) -> None:
    obj = get(db, supplier_id)
    db.delete(obj)
    db.commit()

def add_product(db: Session, supplier_id: str, product_id: str) -> Supplier:
    _validate_uuid(product_id)
    obj = get(db, supplier_id)
    if product_id not in (obj.product_ids or []):
        obj.product_ids.append(product_id)
        db.commit(); db.refresh(obj)
    return obj

def remove_product(db: Session, supplier_id: str, product_id: str) -> Supplier:
    _validate_uuid(product_id)
    obj = get(db, supplier_id)
    if product_id in (obj.product_ids or []):
        obj.product_ids = [x for x in obj.product_ids if x != product_id]
        db.commit(); db.refresh(obj)
    return obj
