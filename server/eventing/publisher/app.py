# publisher/app.py
import os, json, glob, shutil, uuid
import pika

BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/%2f")
EXCHANGE   = os.getenv("EXCHANGE", "events")
EVENTS_DIR = os.getenv("EVENTS_DIR", "/data/events")
BAD_DIR    = os.getenv("BAD_DIR", "/data/dlq")

os.makedirs(BAD_DIR, exist_ok=True)

def publish(ch, routing_key, doc):
    props = pika.BasicProperties(
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=2,  # persistent
        correlation_id=str(uuid.uuid4()),
        message_id=str(uuid.uuid4())
    )
    ch.basic_publish(EXCHANGE, routing_key, json.dumps(doc).encode("utf-8"), properties=props)

def infer_rk(path, doc):
    rk = (doc.get("routing_key") or "").strip()
    if rk:
        return rk
    name = os.path.basename(path).lower()
    if name.startswith("supplier"):
        return "supplier.init"
    if name.startswith("product"):
        return "product.init"
    raise ValueError("No routing_key found; add 'routing_key' in file or rename file with supplier*/product* prefix")

def main():
    params = pika.URLParameters(BROKER_URL)
    cx = pika.BlockingConnection(params)
    ch = cx.channel()
    ch.exchange_declare(EXCHANGE, durable=True, exchange_type="topic")

    for path in glob.glob(os.path.join(EVENTS_DIR, "*.json")):
        try:
            with open(path, "r", encoding="utf-8") as f:
                doc = json.load(f)
            rk = infer_rk(path, doc)
            publish(ch, rk, doc)
            print(f"published {os.path.basename(path)} -> {rk}")
        except Exception as e:
            bad = os.path.join(BAD_DIR, os.path.basename(path))
            shutil.move(path, bad)
            print(f"[BAD] {os.path.basename(path)} -> {bad} ({e})")

    cx.close()

if __name__ == "__main__":
    main()
