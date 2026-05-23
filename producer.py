"""
producer.py
Generates fake stock tick events and publishes them to Kafka topic 'stock-ticks'.
Publishes one event per symbol every 500ms.
"""

import json
import time
import random
import logging
from datetime import datetime, timezone
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "stock-ticks"

SYMBOLS = {
    "AAPL":  {"base_price": 185.0,  "volatility": 0.002},
    "MSFT":  {"base_price": 415.0,  "volatility": 0.0015},
    "GOOG":  {"base_price": 175.0,  "volatility": 0.002},
    "AMZN":  {"base_price": 195.0,  "volatility": 0.0025},
    "META":  {"base_price": 520.0,  "volatility": 0.003},
}

# Tracks last price per symbol for random walk
_last_prices = {sym: data["base_price"] for sym, data in SYMBOLS.items()}


def generate_tick(symbol: str) -> dict:
    """Generate a single stock tick using a random walk model."""
    vol = SYMBOLS[symbol]["volatility"]
    last = _last_prices[symbol]
    change = last * random.gauss(0, vol)
    new_price = round(max(last + change, 1.0), 2)
    _last_prices[symbol] = new_price

    return {
        "symbol":    symbol,
        "price":     new_price,
        "volume":    random.randint(100, 5000),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def delivery_report(err, msg):
    if err:
        log.error(f"Delivery failed for {msg.key()}: {err}")


def run():
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    log.info(f"Connected to Kafka at {KAFKA_BOOTSTRAP}, publishing to '{TOPIC}'")

    published = 0
    try:
        while True:
            for symbol in SYMBOLS:
                tick = generate_tick(symbol)
                producer.produce(
                    topic=TOPIC,
                    key=symbol,
                    value=json.dumps(tick),
                    callback=delivery_report,
                )
                published += 1

            producer.poll(0)

            if published % 50 == 0:
                log.info(f"Published {published} ticks so far")

            time.sleep(0.5)

    except KeyboardInterrupt:
        log.info("Shutting down producer...")
    finally:
        producer.flush()
        log.info(f"Done. Total ticks published: {published}")


if __name__ == "__main__":
    run()
