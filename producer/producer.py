import json
import time

from kafka import KafkaProducer

# The setup remains identical
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

print("Producer started. Sending 10 messages...")

for i in range(10):
    data = {"event": "agent_task", "id": i}

    # .send() returns a future; it is asynchronous by default
    producer.send("test-topic", data)
    print(f"Sent: {data}")

    time.sleep(1)

# Ensure all messages are actually sent before the script exits
producer.flush()
print("All messages flushed to broker.")
