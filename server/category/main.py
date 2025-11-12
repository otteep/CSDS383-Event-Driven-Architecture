import logging
from fastapi import FastAPI, Depends, status
from sqlalchemy.orm import Session

from config import settings
from database import Base, engine
import crud
from deps import get_db
from schemas import CategoryCreate, CategoryUpdate, CategoryOut, LinkProductOp
from sync import (
    sync_add_category_to_products,
    sync_remove_category_from_products,
    sync_replace_category_products,
)

# Initialize DB schema
Base.metadata.create_all(bind=engine)

# Logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
log = logging.getLogger("category.service")

app = FastAPI(
    title="Category Service",
    version="1.0.0",
    description="CRUD for categories with bidirectional sync to Product service.",
)

# ---- Health ----
@app.get("/health")
def health():
    return {"status": "ok", "service": "category", "version": "1.0.0"}

# ---- CRUD ----
@app.post("/categories", response_model=CategoryOut, status_code=status.HTTP_201_CREATED)
def create_category(payload: CategoryCreate, db: Session = Depends(get_db)):
    created = crud.create(db, payload)
    if created.product_ids:
        sync_add_category_to_products(created.id, created.product_ids)
    return created

@app.get("/categories", response_model=list[CategoryOut])
def list_categories(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.list_all(db, skip=skip, limit=limit)

@app.get("/categories/{category_id}", response_model=CategoryOut)
def read_category(category_id: str, db: Session = Depends(get_db)):
    return crud.get(db, category_id)

@app.put("/categories/{category_id}", response_model=CategoryOut)
def put_category(category_id: str, payload: CategoryUpdate, db: Session = Depends(get_db)):
    existing = crud.get(db, category_id)
    old_ids = list(existing.product_ids or [])
    updated = crud.update(db, category_id, payload)
    if payload.product_ids is not None:
        sync_replace_category_products(category_id, old_ids, updated.product_ids or [])
    return updated

@app.patch("/categories/{category_id}", response_model=CategoryOut)
def patch_category(category_id: str, payload: CategoryUpdate, db: Session = Depends(get_db)):
    existing = crud.get(db, category_id)
    old_ids = list(existing.product_ids or [])
    updated = crud.update(db, category_id, payload)
    if payload.product_ids is not None:
        sync_replace_category_products(category_id, old_ids, updated.product_ids or [])
    return updated

@app.delete("/categories/{category_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_category(category_id: str, db: Session = Depends(get_db)):
    existing = crud.get(db, category_id)
    prod_ids = list(existing.product_ids or [])
    crud.delete(db, category_id)
    if prod_ids:
        sync_remove_category_from_products(category_id, prod_ids)
    return None

# ---- Relationship helpers (used by Product service & optionally clients) ----
@app.post("/categories/{category_id}/products", response_model=CategoryOut)
def link_product(category_id: str, op: LinkProductOp, db: Session = Depends(get_db)):
    cat = crud.add_product(db, category_id, op.product_id)
    # Let Product service handle its own side when it calls us; double-sync is harmless (idempotent).
    return cat

@app.delete("/categories/{category_id}/products/{product_id}", response_model=CategoryOut)
def unlink_product(category_id: str, product_id: str, db: Session = Depends(get_db)):
    cat = crud.remove_product(db, category_id, product_id)
    return cat
