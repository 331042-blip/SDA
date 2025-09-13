#!/usr/bin/env python3
"""
consumer_combined.py
Consumes employee data from Kafka (employee-topic) and:
1. Inserts raw data into MongoDB Atlas (dept_metrics collection).
2. Generates alerts based on rules and inserts them into alerts collection.
"""

import json
from datetime import datetime
from kafka import KafkaConsumer
from pymongo import MongoClient

# ==============================
# Config
# ==============================
KAFKA_BROKER = "localhost:9092"
TOPIC = "employee-topic"

# ✅ Your Atlas connection string
MONGO_URI = ""
DB_NAME = "employee_db"

# ==============================
# MongoDB setup
# ==============================
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
dept_metrics = db["dept_metrics"]   # raw records
alerts = db["alerts"]               # alerts

# ==============================
# Kafka consumer setup
# ==============================
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=[KAFKA_BROKER],
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="combined-consumer",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)

print("✅ Combined Consumer started (writes to dept_metrics + alerts)...")

# ==============================
# Main loop
# ==============================
for msg in consumer:
    rec = msg.value
    emp_id = rec.get("Employee_ID")
    name = rec.get("Full_Name")
    dept = rec.get("Department")
    perf = float(rec.get("Performance_Rating", 0) or 0)
    salary = float(rec.get("Salary_INR", 0) or 0)
    status = rec.get("Status", "")

    # --- Insert raw record into dept_metrics ---
    try:
        rec["_kafka_ingest_ts"] = datetime.utcnow()
        dept_metrics.insert_one(rec)
        print(f"📥 Inserted into dept_metrics: {emp_id} - {name}")
    except Exception as e:
        print("❌ Failed to insert raw record:", e)

    # --- Generate alerts ---
    alert_list = []

    if perf <= 2:
        alert_list.append({
            "timestamp": datetime.utcnow(),
            "type": "low_performance",
            "employee_id": emp_id,
            "name": name,
            "department": dept,
            "performance": perf,
            "message": f"Low performance ({perf}) for {name}"
        })
        print(f"🚨 ALERT: Low performance ({perf}) for {name} [{emp_id}] in {dept}")

    if salary > 5_000_000:
        alert_list.append({
            "timestamp": datetime.utcnow(),
            "type": "high_salary",
            "employee_id": emp_id,
            "name": name,
            "department": dept,
            "salary": salary,
            "message": f"High salary ({salary}) for {name}"
        })
        print(f"💰 ALERT: High salary ({salary}) for {name} [{emp_id}] in {dept}")

    if status.lower() == "resigned":
        alert_list.append({
            "timestamp": datetime.utcnow(),
            "type": "resignation",
            "employee_id": emp_id,
            "name": name,
            "department": dept,
            "message": f"Employee {name} has resigned"
        })
        print(f"⚠️ ALERT: Employee {name} [{emp_id}] in {dept} has resigned")

    # --- Insert alerts into alerts collection ---
    if alert_list:
        try:
            alerts.insert_many(alert_list)
            print(f"🟢 Inserted {len(alert_list)} alerts into alerts collection")
        except Exception as e:
            print("❌ Failed to insert alerts:", e)
