import logging
from fastapi import FastAPI, Depends, status
from sqlalchemy.orm import Session

from config import settings
from database import Base, engine
from deps import get_db
import crud
from schemas import ImageCreate, ImageUpdate, ImageOut
from sync import sync_link_to_product, sync_unlink_from_product

# Initialize DB
Base.metadata.create_all(bind=engine)

# Logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
log = logging.getLogger("image.service")

app = FastAPI(
    title="Image Service",
    version="1.0.0",
    description="CRUD for images with single product association and bidirectional sync with Product service.",
)

# ---- Health
@app.get("/health")
def health():
    return {"status": "ok", "service": "image", "version": "1.0.0"}

# ---- CRUD
@app.post("/images", response_model=ImageOut, status_code=status.HTTP_201_CREATED)
def create_image(payload: ImageCreate, db: Session = Depends(get_db)):
    created = crud.create(db, payload)
    # If created with product_id, sync to Product
    if created.product_id:
        sync_link_to_product(created.product_id, created.id)
    return created

@app.get("/images", response_model=list[ImageOut])
def list_images(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.list_all(db, skip=skip, limit=limit)

@app.get("/images/{image_id}", response_model=ImageOut)
def read_image(image_id: str, db: Session = Depends(get_db)):
    return crud.get(db, image_id)

@app.put("/images/{image_id}", response_model=ImageOut)
def put_image(image_id: str, payload: ImageUpdate, db: Session = Depends(get_db)):
    existing = crud.get(db, image_id)
    old_pid = existing.product_id
    updated = crud.update(db, image_id, payload)

    # If product_id explicitly changed, bidirectional sync
    if "product_id" in payload.model_fields_set:
        new_pid = updated.product_id
        if old_pid and old_pid != new_pid:
            sync_unlink_from_product(old_pid, image_id)
        if new_pid and new_pid != old_pid:
            sync_link_to_product(new_pid, image_id)
    return updated

@app.patch("/images/{image_id}", response_model=ImageOut)
def patch_image(image_id: str, payload: ImageUpdate, db: Session = Depends(get_db)):
    return put_image(image_id, payload, db)

@app.delete("/images/{image_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_image(image_id: str, db: Session = Depends(get_db)):
    old_pid = crud.delete(db, image_id)
    if old_pid:
        sync_unlink_from_product(old_pid, image_id)
    return None
