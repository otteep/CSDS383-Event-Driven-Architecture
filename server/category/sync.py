import logging
import requests
from typing import Iterable
from config import settings

log = logging.getLogger("category.sync")

def _safe_request(method: str, url: str, json=None):
    try:
        resp = requests.request(method=method, url=url, json=json, timeout=settings.HTTP_TIMEOUT)
        if resp.status_code >= 400:
            log.warning("Sync call failed %s %s -> %s %s", method, url, resp.status_code, resp.text)
    except Exception as e:
        log.warning("Sync call exception %s %s: %s", method, url, e)

# Product service contract (via gateway or direct):
#   POST   /products/{product_id}/categories/{category_id}     (link)
#   DELETE /products/{product_id}/categories/{category_id}     (unlink)

def sync_add_category_to_products(category_id: str, product_ids: Iterable[str]) -> None:
    for pid in product_ids or []:
        url = f"{settings.PRODUCT_BASE_URL}/{pid}/categories/{category_id}"
        _safe_request("POST", url)

def sync_remove_category_from_products(category_id: str, product_ids: Iterable[str]) -> None:
    for pid in product_ids or []:
        url = f"{settings.PRODUCT_BASE_URL}/{pid}/categories/{category_id}"
        _safe_request("DELETE", url)

def sync_replace_category_products(category_id: str, old_ids: Iterable[str], new_ids: Iterable[str]) -> None:
    old_set, new_set = set(old_ids or []), set(new_ids or [])
    to_add = new_set - old_set
    to_remove = old_set - new_set
    if to_add:
        sync_add_category_to_products(category_id, to_add)
    if to_remove:
        sync_remove_category_from_products(category_id, to_remove)
