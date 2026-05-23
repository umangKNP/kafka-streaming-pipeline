"""
consumer.py
Reads stock-ticks from Kafka, computes per-symbol VWAP over 1-minute windows,
upserts into Postgres. Failed messages (3 retries) go to dead letter queue topic.
"""

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
from confluent_kafka import Consumer, Producer, KafkaError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP  = "localhost:9092"
TOPIC            = "stock-ticks"
DLQ_TOPIC        = "stock-ticks-dlq"
GROUP_ID         = "stock-aggregator"
MAX_RETRIES      = 3

PG_DSN = "host=localhost port=5432 dbname=stockdb user=pipeline password=pipeline"


def get_window_start(ts: datetime) -> datetime:
    """Truncate timestamp to the nearest 1-minute boundary."""
    return ts.replace(second=0, microsecond=0)


class VWAPAccumulator:
    """Holds running totals for VWAP computation per (symbol, window)."""

    def __init__(self):
        # key: (symbol, window_start) -> {"price_volume_sum": float, "volume": int, "count": int}
        self._buckets: dict = defaultdict(lambda: {"pv_sum": 0.0, "volume": 0, "count": 0})
        self._current_window: datetime | None = None

    def add(self, symbol: str, price: float, volume: int, ts: datetime):
        window = get_window_start(ts)
        key = (symbol, window)
        b = self._buckets[key]
        b["pv_sum"]  += price * volume
        b["volume"]  += volume
        b["count"]   += 1
        self._current_window = window

    def flush_completed_windows(self, now: datetime) -> list[dict]:
        """Return and remove all windows older than current minute."""
        current = get_window_start(now)
        ready = []
        for (symbol, window), b in list(self._buckets.items()):
            if window < current:
                vwap = round(b["pv_sum"] / b["volume"], 4) if b["volume"] > 0 else 0
                ready.append({
                    "symbol":       symbol,
                    "window_start": window,
                    "vwap":         vwap,
                    "total_volume": b["volume"],
                    "trade_count":  b["count"],
                })
                del self._buckets[(symbol, window)]
        return ready


class DBWriter:
    def __init__(self, dsn: str):
        self.conn = psycopg2.connect(dsn)
        self.conn.autocommit = False
        log.info("Connected to Postgres")

    def upsert_aggregates(self, rows: list[dict]):
        if not rows:
            return
        sql = """
            INSERT INTO price_aggregates
                (symbol, window_start, vwap, total_volume, trade_count, updated_at)
            VALUES
                (%(symbol)s, %(window_start)s, %(vwap)s, %(total_volume)s, %(trade_count)s, NOW())
            ON CONFLICT (symbol, window_start)
            DO UPDATE SET
                vwap         = EXCLUDED.vwap,
                total_volume = EXCLUDED.total_volume,
                trade_count  = EXCLUDED.trade_count,
                updated_at   = NOW()
        """
        with self.conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, sql, rows)
        self.conn.commit()
        log.info(f"Upserted {len(rows)} aggregate rows")

    def write_dlq(self, raw: str, reason: str):
        sql = """
            INSERT INTO dead_letter_queue (raw_message, error_reason)
            VALUES (%s, %s)
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (raw, reason))
        self.conn.commit()


def run():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id":          GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    dlq_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    consumer.subscribe([TOPIC])

    db      = DBWriter(PG_DSN)
    acc     = VWAPAccumulator()
    retries: dict = defaultdict(int)   # offset -> retry count

    log.info(f"Subscribed to '{TOPIC}', consumer group '{GROUP_ID}'")

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Flush any completed windows even when idle
                ready = acc.flush_completed_windows(datetime.now(timezone.utc))
                if ready:
                    db.upsert_aggregates(ready)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                log.error(f"Kafka error: {msg.error()}")
                continue

            raw = msg.value().decode("utf-8")
            offset = msg.offset()

            try:
                tick = json.loads(raw)
                ts   = datetime.fromisoformat(tick["timestamp"])
                acc.add(tick["symbol"], float(tick["price"]), int(tick["volume"]), ts)

                # Flush windows on every message
                ready = acc.flush_completed_windows(datetime.now(timezone.utc))
                if ready:
                    db.upsert_aggregates(ready)

                consumer.commit(message=msg)
                retries.pop(offset, None)

            except Exception as e:
                retries[offset] += 1
                log.warning(f"Error processing offset {offset} (attempt {retries[offset]}): {e}")

                if retries[offset] >= MAX_RETRIES:
                    reason = f"Failed after {MAX_RETRIES} retries: {e}"
                    log.error(f"Sending offset {offset} to DLQ — {reason}")
                    dlq_producer.produce(DLQ_TOPIC, key=msg.key(), value=raw)
                    dlq_producer.flush()
                    db.write_dlq(raw, reason)
                    consumer.commit(message=msg)
                    retries.pop(offset, None)

    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    finally:
        consumer.close()
        log.info("Consumer closed")


if __name__ == "__main__":
    run()
