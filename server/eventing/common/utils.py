import os, json, sqlite3, re
from typing import Dict, Any

IDEM_DB = os.getenv("IDEMPOTENCY_DB", "./idempotency.db")
os.makedirs(os.path.dirname(IDEM_DB) or ".", exist_ok=True)

def _conn():
    c = sqlite3.connect(IDEM_DB)
    c.execute("CREATE TABLE IF NOT EXISTS seen (event_id TEXT PRIMARY KEY, payload TEXT)")
    c.commit()
    return c

def seen(event_id: str) -> bool:
    c = _conn()
    r = c.execute("SELECT 1 FROM seen WHERE event_id=?", (event_id,)).fetchone()
    c.close()
    return r is not None

def remember(event_id: str, payload: Dict[str, Any]) -> None:
    c = _conn()
    c.execute("INSERT OR IGNORE INTO seen(event_id, payload) VALUES(?,?)", (event_id, json.dumps(payload)))
    c.commit()
    c.close()

EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
def is_email(s: str) -> bool:
    return bool(EMAIL_RE.match(s or ""))
