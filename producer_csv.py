#!/usr/bin/env python3
"""
producer_csv.py
Reads rows from a CSV (or TSV) file and streams them to Kafka periodically.
Adjust CSV_FILE, TOPIC, KAFKA_BROKER and timing constants as needed.
"""
import csv
import time
import json
import sys
from kafka import KafkaProducer, errors

KAFKA_BROKER = "localhost:9092"
TOPIC = "employee-topic"
CSV_FILE = "employees.csv"
SLEEP_BETWEEN_MESSAGES = 1         # seconds between producing individual rows
SLEEP_AFTER_EOF = 60               # seconds to wait after finishing file before restarting

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            retries=5
        )
        # Touch metadata to ensure broker availability (will raise if broker missing)
        producer.partitions_for(TOPIC)
        print("✅ Connected to Kafka broker at", KAFKA_BROKER)
        return producer
    except errors.NoBrokersAvailable:
        print("❌ Kafka broker not available at", KAFKA_BROKER)
        sys.exit(1)
    except Exception as e:
        print("❌ Error connecting to Kafka:", e)
        sys.exit(1)

def detect_dialect(path):
    import io
    with open(path, "r", encoding="utf-8") as f:
        sample = f.read(2048)
        f.seek(0)
        try:
            sniffer = csv.Sniffer()
            dialect = sniffer.sniff(sample, delimiters=",\t;")
            return dialect
        except Exception:
            # fallback to comma
            class D:
                delimiter = ","
            return D()

def normalize_row(row):
    # Try to cast Salary_INR and Experience_Years to numeric types when possible
    for k in ("Salary_INR", "Experience_Years"):
        if k in row and row[k] is not None and row[k] != "":
            try:
                if "." in row[k]:
                    row[k] = float(row[k])
                else:
                    row[k] = int(row[k])
            except Exception:
                pass
    return row

def stream_csv():
    producer = create_producer()
    while True:
        try:
            dialect = detect_dialect(CSV_FILE)
            with open(CSV_FILE, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f, delimiter=dialect.delimiter)
                for row in reader:
                    row = normalize_row(row)
                    # Add an ingestion timestamp (UTC)
                    row["_ingest_ts_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    try:
                        producer.send(TOPIC, value=row)
                        print("📤 Produced:", row)
                    except Exception as e:
                        print("❌ Failed to send message:", e)
                    time.sleep(SLEEP_BETWEEN_MESSAGES)
            print(f"⚪ Reached end of file ({CSV_FILE}). Sleeping {SLEEP_AFTER_EOF}s before restarting...")
            time.sleep(SLEEP_AFTER_EOF)
        except KeyboardInterrupt:
            print("\n🛑 Producer stopped by user.")
            break
        except FileNotFoundError:
            print(f"❌ CSV file not found: {CSV_FILE}. Please place the file in the same folder and try again.")
            time.sleep(10)
        except Exception as e:
            print("❌ Producer error:", e)
            time.sleep(10)

if __name__ == "__main__":
    stream_csv()