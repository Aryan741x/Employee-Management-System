import json
import time
from kafka import KafkaProducer

# Initialize Kafka producer (kafka)
producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Load message data from LOCAL file
with open("data/messages.json", "r") as file:
    messages = json.load(file)

# Kafka topic name
topic = "employee-messages"

print("Sending messages to Kafka (kafka:9092)...\n")

try:
    for msg in messages:
        # Basic validation
        if not all(k in msg for k in ("sender", "receiver", "message")):
            print(f"Skipped invalid message: {msg}")
            continue

        key = msg["sender"]
        value = msg

        # Send to Kafka
        producer.send(topic, key=key, value=value)
        print(f"Sent: {json.dumps(value)}")

        time.sleep(1)

except KeyboardInterrupt:
    print("\nInterrupted by user.")

finally:
    print("\nFlushing remaining messages...")
    producer.flush()
    print("All messages flushed and sent.")