from sqlalchemy.orm import Session
from fastapi import HTTPException, status
from typing import Optional, List
import uuid

from models import Image
from schemas import ImageCreate, ImageUpdate

def _validate_uuid_opt(id_str: Optional[str]) -> None:
    if id_str is None:
        return
    try:
        uuid.UUID(str(id_str))
    except Exception:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=f"Invalid UUID: {id_str}")

def get(db: Session, image_id: str) -> Image:
    obj = db.query(Image).filter(Image.id == image_id).first()
    if not obj:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Image not found")
    return obj

def list_all(db: Session, skip: int = 0, limit: int = 100) -> List[Image]:
    return db.query(Image).offset(skip).limit(limit).all()

def create(db: Session, payload: ImageCreate) -> Image:
    iid = payload.id or str(uuid.uuid4())
    _validate_uuid_opt(iid)
    _validate_uuid_opt(payload.product_id)

    obj = Image(
        id=iid,
        product_id=payload.product_id,
        url=str(payload.url),
    )
    db.add(obj)
    db.commit()
    db.refresh(obj)
    return obj

def update(db: Session, image_id: str, payload: ImageUpdate) -> Image:
    obj = get(db, image_id)
    if payload.url is not None:
        obj.url = str(payload.url)
    # product_id can be UUID or None (detach)
    if "product_id" in payload.model_fields_set:
        _validate_uuid_opt(payload.product_id)
        obj.product_id = payload.product_id
    db.commit()
    db.refresh(obj)
    return obj

def delete(db: Session, image_id: str) -> Optional[str]:
    obj = get(db, image_id)
    old_pid = obj.product_id
    db.delete(obj)
    db.commit()
    return old_pid
