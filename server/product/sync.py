import logging
import requests
from typing import Iterable
from config import settings

log = logging.getLogger("product.sync")

def _safe_request(method: str, url: str, json=None):
    try:
        resp = requests.request(method=method, url=url, json=json, timeout=settings.HTTP_TIMEOUT)
        if resp.status_code >= 400:
            log.warning("Sync %s %s -> %s %s", method, url, resp.status_code, resp.text)
    except Exception as e:
        log.warning("Sync exception %s %s: %s", method, url, e)

# ---- SUPPLIER bidirectional ----
# Supplier service contract:
#   POST   /suppliers/{sid}/products  {"product_id":"<pid>"}
#   DELETE /suppliers/{sid}/products/{pid}
def sync_add_product_to_suppliers(pid: str, sids: Iterable[str]):  # attach
    for sid in sids or []:
        _safe_request("POST", f"{settings.SUPPLIER_BASE_URL}/{sid}/products", json={"product_id": pid})

def sync_remove_product_from_suppliers(pid: str, sids: Iterable[str]):  # detach
    for sid in sids or []:
        _safe_request("DELETE", f"{settings.SUPPLIER_BASE_URL}/{sid}/products/{pid}")

# ---- CATEGORY bidirectional ----
# Product <-> Category:
#   POST   /products/{pid}/categories/{cid}
#   DELETE /products/{pid}/categories/{cid}
def sync_add_product_to_categories(pid: str, cids: Iterable[str]):
    for cid in cids or []:
        _safe_request("POST", f"{settings.CATEGORY_BASE_URL}/{cid}/products", json={"product_id": pid})

def sync_remove_product_from_categories(pid: str, cids: Iterable[str]):
    for cid in cids or []:
        _safe_request("DELETE", f"{settings.CATEGORY_BASE_URL}/{cid}/products/{pid}")

# ---- IMAGE bidirectional ----
# Image service contract (single resource owns product link):
#   PATCH /images/{iid} {"product_id":"<pid>"} to attach
#   PATCH /images/{iid} {"product_id":null}   to detach
def sync_attach_images_to_product(pid: str, iids: Iterable[str]):
    for iid in iids or []:
        _safe_request("PATCH", f"{settings.IMAGE_BASE_URL}/{iid}", json={"product_id": pid})

def sync_detach_images_from_product(iids: Iterable[str]):
    for iid in iids or []:
        _safe_request("PATCH", f"{settings.IMAGE_BASE_URL}/{iid}", json={"product_id": None})
