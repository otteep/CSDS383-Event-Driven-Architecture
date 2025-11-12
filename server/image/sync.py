import logging
import requests
from config import settings

log = logging.getLogger("image.sync")

def _safe_request(method: str, url: str, json=None):
    try:
        resp = requests.request(method=method, url=url, json=json, timeout=settings.HTTP_TIMEOUT)
        if resp.status_code >= 400:
            log.warning("Sync %s %s -> %s %s", method, url, resp.status_code, resp.text)
    except Exception as e:
        log.warning("Sync exception %s %s: %s", method, url, e)

# Product service contract (already used by Product service too, idempotent):
#   POST   /products/{pid}/images/{iid}   -> link
#   DELETE /products/{pid}/images/{iid}   -> unlink

def sync_link_to_product(product_id: str, image_id: str) -> None:
    _safe_request("POST", f"{settings.PRODUCT_BASE_URL}/{product_id}/images/{image_id}")

def sync_unlink_from_product(product_id: str, image_id: str) -> None:
    _safe_request("DELETE", f"{settings.PRODUCT_BASE_URL}/{product_id}/images/{image_id}")
