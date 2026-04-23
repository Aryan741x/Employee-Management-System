import json
import time
from kafka import KafkaProducer

# ─────────────────────────────────────────────────────────────
# Kafka Producer  –  connects to kafka:9092 (Docker container)
# ─────────────────────────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# FIX: absolute path so this works inside the Docker container
with open("/opt/airflow/data/messages.json", "r") as file:
    messages = json.load(file)

topic = "employee-messages"

print(f"Sending {len(messages)} messages to Kafka topic '{topic}' at kafka:9092...\n")

sent = 0
skipped = 0

try:
    for msg in messages:
        # Validate required fields
        if not all(k in msg for k in ("sender", "receiver", "message")):
            print(f"  Skipped invalid message: {msg}")
            skipped += 1
            continue

        key   = msg["sender"]
        value = msg

        producer.send(topic, key=key, value=value)
        sent += 1

        if sent % 100 == 0:
            print(f"  Sent {sent} messages so far...")

        time.sleep(0.05)  # 50 ms between messages (faster than 1s but polite to Kafka)

except KeyboardInterrupt:
    print("\nInterrupted by user.")

finally:
    print(f"\nFlushing remaining messages...")
    producer.flush()
    print(f"\nDone. Sent: {sent}  |  Skipped: {skipped}")