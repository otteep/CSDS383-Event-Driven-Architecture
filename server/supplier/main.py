import logging
from fastapi import FastAPI, Depends, status
from sqlalchemy.orm import Session

from config import settings
from database import Base, engine
from deps import get_db
import crud
from schemas import SupplierCreate, SupplierUpdate, SupplierOut, LinkProductOp
from sync import (
    sync_add_supplier_to_products,
    sync_remove_supplier_from_products,
    sync_replace_supplier_products,
)

# Initialize DB schema
Base.metadata.create_all(bind=engine)

# Logging
logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO))
log = logging.getLogger("supplier.service")

app = FastAPI(
    title="Supplier Service",
    version="1.0.0",
    description="CRUD for suppliers with validations and bidirectional sync to Product service.",
)

# ---- Health
@app.get("/health")
def health():
    return {"status": "ok", "service": "supplier", "version": "1.0.0"}

# ---- CRUD
@app.post("/suppliers", response_model=SupplierOut, status_code=status.HTTP_201_CREATED)
def create_supplier(payload: SupplierCreate, db: Session = Depends(get_db)):
    created = crud.create(db, payload)
    if created.product_ids:
        sync_add_supplier_to_products(created.id, created.product_ids)
    return created

@app.get("/suppliers", response_model=list[SupplierOut])
def list_suppliers(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    return crud.list_all(db, skip=skip, limit=limit)

@app.get("/suppliers/{supplier_id}", response_model=SupplierOut)
def read_supplier(supplier_id: str, db: Session = Depends(get_db)):
    return crud.get(db, supplier_id)

@app.put("/suppliers/{supplier_id}", response_model=SupplierOut)
def put_supplier(supplier_id: str, payload: SupplierUpdate, db: Session = Depends(get_db)):
    existing = crud.get(db, supplier_id)
    old_ids = list(existing.product_ids or [])
    updated = crud.update(db, supplier_id, payload)
    if payload.product_ids is not None:
        sync_replace_supplier_products(supplier_id, old_ids, updated.product_ids or [])
    return updated

@app.patch("/suppliers/{supplier_id}", response_model=SupplierOut)
def patch_supplier(supplier_id: str, payload: SupplierUpdate, db: Session = Depends(get_db)):
    existing = crud.get(db, supplier_id)
    old_ids = list(existing.product_ids or [])
    updated = crud.update(db, supplier_id, payload)
    if payload.product_ids is not None:
        sync_replace_supplier_products(supplier_id, old_ids, updated.product_ids or [])
    return updated

@app.delete("/suppliers/{supplier_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_supplier(supplier_id: str, db: Session = Depends(get_db)):
    existing = crud.get(db, supplier_id)
    prod_ids = list(existing.product_ids or [])
    crud.delete(db, supplier_id)
    if prod_ids:
        sync_remove_supplier_from_products(supplier_id, prod_ids)
    return None

# ---- Relationship endpoints (used by Product service & optionally clients)
@app.post("/suppliers/{supplier_id}/products", response_model=SupplierOut)
def link_product(supplier_id: str, op: LinkProductOp, db: Session = Depends(get_db)):
    sup = crud.add_product(db, supplier_id, op.product_id)
    # Product service will call its own side; double-calls are harmless (idempotent).
    return sup

@app.delete("/suppliers/{supplier_id}/products/{product_id}", response_model=SupplierOut)
def unlink_product(supplier_id: str, product_id: str, db: Session = Depends(get_db)):
    sup = crud.remove_product(db, supplier_id, product_id)
    return sup
