# scripts/topology.py
import os, pika

BROKER_URL = os.getenv("BROKER_URL", "amqp://guest:guest@rabbitmq:5672/")
EXCHANGE   = os.getenv("EXCHANGE", "events")

Q_SUPPLIER_INIT  = os.getenv("Q_SUPPLIER_INIT", "supplier.init.q")
Q_PRODUCT_INIT   = os.getenv("Q_PRODUCT_INIT",  "product.init.q")
Q_SUPPLIER_VALID = os.getenv("Q_SUPPLIER_VALID","supplier.valid.q")

DLX_INIT    = os.getenv("DLX_INIT", "dlx.init")
DLQ_INIT    = os.getenv("DLQ_INIT", "dlq.init.q")
DLX_PRODUCT = os.getenv("DLX_PRODUCT", "dlx.product")
DLQ_PRODUCT = os.getenv("DLQ_PRODUCT", "dlq.product.q")

params = pika.URLParameters(BROKER_URL)
cx = pika.BlockingConnection(params)
ch = cx.channel()

# Exchanges
ch.exchange_declare(EXCHANGE, durable=True, exchange_type="topic")
ch.exchange_declare(DLX_INIT, exchange_type="fanout", durable=True)
ch.exchange_declare(DLX_PRODUCT, exchange_type="fanout", durable=True)

# DLQs
ch.queue_declare(DLQ_INIT, durable=True)
ch.queue_bind(DLQ_INIT, DLX_INIT, routing_key="")
ch.queue_declare(DLQ_PRODUCT, durable=True)
ch.queue_bind(DLQ_PRODUCT, DLX_PRODUCT, routing_key="")

# Supplier init queue (dead-letter to DLX_INIT)
ch.queue_declare(Q_SUPPLIER_INIT, durable=True, arguments={
    "x-dead-letter-exchange": DLX_INIT
})
ch.queue_bind(Q_SUPPLIER_INIT, EXCHANGE, routing_key="supplier.init")

# Product init queue (dead-letter to DLX_PRODUCT)
ch.queue_declare(Q_PRODUCT_INIT, durable=True, arguments={
    "x-dead-letter-exchange": DLX_PRODUCT
})
ch.queue_bind(Q_PRODUCT_INIT, EXCHANGE, routing_key="product.init")

# Supplier valid (downstream fan-out topic)
ch.queue_declare(Q_SUPPLIER_VALID, durable=True)
ch.queue_bind(Q_SUPPLIER_VALID, EXCHANGE, routing_key="supplier.valid")

print("Topology ready.")
cx.close()
