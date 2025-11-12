import logging
from fastapi import FastAPI, Depends, status, HTTPException
from sqlalchemy.orm import Session

from config import settings
from database import Base, engine
from deps import get_db
import crud
from schemas import ProductCreate, ProductUpdate, ProductOut
from sync import (
    sync_add_product_to_suppliers,
    sync_remove_product_from_suppliers,
    sync_add_product_to_categories,
    sync_remove_product_from_categories,
    sync_attach_images_to_product,
    sync_detach_images_from_product,
)

# DB schema init
Base.metadata.create_all(bind=engine)

# Logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
log = logging.getLogger("product.service")

app = FastAPI(
    title="Product Service",
    version="1.0.0",
    description="CRUD for products with validations and bidirectional sync to Supplier, Category, and Image services.",
)

# ---- Health
@app.get("/health")
def health():
    return {"status": "ok", "service": "product", "version": "1.0.0"}

# ---- CRUD
@app.post("/products", response_model=ProductOut, status_code=status.HTTP_201_CREATED)
def create_product(payload: ProductCreate, db: Session = Depends(get_db)):
    created = crud.create(db, payload)
    if created.supplier_ids:
        sync_add_product_to_suppliers(created.id, created.supplier_ids)
    if created.category_ids:
        sync_add_product_to_categories(created.id, created.category_ids)
    if created.image_ids:
        sync_attach_images_to_product(created.id, created.image_ids)
    return created

@app.get("/products", response_model=list[ProductOut])
def list_products(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.list_all(db, skip=skip, limit=limit)

@app.get("/products/{product_id}", response_model=ProductOut)
def read_product(product_id: str, db: Session = Depends(get_db)):
    return crud.get(db, product_id)

@app.put("/products/{product_id}", response_model=ProductOut)
def put_product(product_id: str, payload: ProductUpdate, db: Session = Depends(get_db)):
    existing = crud.get(db, product_id)
    old_sup, old_cat, old_img = set(existing.supplier_ids or []), set(existing.category_ids or []), set(existing.image_ids or [])
    updated = crud.update(db, product_id, payload)

    if payload.supplier_ids is not None:
        new_sup = set(updated.supplier_ids or [])
        sync_add_product_to_suppliers(product_id, new_sup - old_sup)
        sync_remove_product_from_suppliers(product_id, old_sup - new_sup)

    if payload.category_ids is not None:
        new_cat = set(updated.category_ids or [])
        sync_add_product_to_categories(product_id, new_cat - old_cat)
        sync_remove_product_from_categories(product_id, old_cat - new_cat)

    if payload.image_ids is not None:
        new_img = set(updated.image_ids or [])
        if new_img - old_img:
            sync_attach_images_to_product(product_id, new_img - old_img)
        if old_img - new_img:
            sync_detach_images_from_product(old_img - new_img)

    return updated

@app.patch("/products/{product_id}", response_model=ProductOut)
def patch_product(product_id: str, payload: ProductUpdate, db: Session = Depends(get_db)):
    return put_product(product_id, payload, db)

@app.delete("/products/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_product(product_id: str, db: Session = Depends(get_db)):
    existing = crud.get(db, product_id)
    sids = list(existing.supplier_ids or [])
    cids = list(existing.category_ids or [])
    iids = list(existing.image_ids or [])
    
    crud.delete(db, product_id)
    
    # TEMPORARILY DISABLED - other services not running
    # if sids:
    #     sync_remove_product_from_suppliers(product_id, sids)
    # if cids:
    #     sync_remove_product_from_categories(product_id, cids)
    # if iids:
    #     sync_detach_images_from_product(iids)
    
    return None

# ---- Relationship endpoints (used by peer services & optionally clients)
@app.post("/products/{product_id}/suppliers/{supplier_id}", response_model=ProductOut)
def link_supplier(product_id: str, supplier_id: str, db: Session = Depends(get_db)):
    return crud.add_supplier(db, product_id, supplier_id)

@app.delete("/products/{product_id}/suppliers/{supplier_id}", response_model=ProductOut)
def unlink_supplier(product_id: str, supplier_id: str, db: Session = Depends(get_db)):
    return crud.remove_supplier(db, product_id, supplier_id)

@app.post("/products/{product_id}/categories/{category_id}", response_model=ProductOut)
def link_category(product_id: str, category_id: str, db: Session = Depends(get_db)):
    return crud.add_category(db, product_id, category_id)

@app.delete("/products/{product_id}/categories/{category_id}", response_model=ProductOut)
def unlink_category(product_id: str, category_id: str, db: Session = Depends(get_db)):
    return crud.remove_category(db, product_id, category_id)

@app.post("/products/{product_id}/images/{image_id}", response_model=ProductOut)
def link_image(product_id: str, image_id: str, db: Session = Depends(get_db)):
    return crud.add_image(db, product_id, image_id)

@app.delete("/products/{product_id}/images/{image_id}", response_model=ProductOut)
def unlink_image(product_id: str, image_id: str, db: Session = Depends(get_db)):
    return crud.remove_image(db, product_id, image_id)
