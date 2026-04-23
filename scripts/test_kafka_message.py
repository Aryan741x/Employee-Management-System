import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers='kafka:9092',
    key_serializer=lambda k: str(k).encode('utf-8'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

msg = {"sender": "EMP042", "receiver": "EMP099", "message": "This is a violation using IAF9TTGGC2 immediately."}
producer.send("employee-messages", key=msg["sender"], value=msg)
producer.flush()
print("Flagged message sent to Kafka!")
