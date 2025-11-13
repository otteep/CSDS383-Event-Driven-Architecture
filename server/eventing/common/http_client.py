import os, time, logging, requests
from typing import Dict, Any

log = logging.getLogger("http")
TIMEOUT = int(os.getenv("SERVICE_TIMEOUT", "5"))
RETRIES = int(os.getenv("SERVICE_RETRIES", "2"))

def post_json(url: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    attempts = RETRIES + 1
    backoff = 0.5
    for i in range(attempts):
        try:
            resp = requests.post(url, json=payload, timeout=TIMEOUT)
            if 200 <= resp.status_code < 300:
                return resp.json()
            if 400 <= resp.status_code < 500:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            log.warning("HTTP %s from %s; retrying...", resp.status_code, url)
        except Exception as e:
            if i == attempts - 1:
                raise
            log.warning("HTTP error %s (try %s/%s); retrying...", e, i + 1, attempts)
        time.sleep(backoff); backoff *= 2
    raise RuntimeError("unreachable")
