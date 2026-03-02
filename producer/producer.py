import json

from kafka import KafkaProducer
from sseclient import SSEClient as EventSource

# 1. Setup Producer
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

url = "https://stream.wikimedia.org/v2/stream/recentchange"
headers = {"User-Agent": "KafkaWikiBot/1.0 (vimal@example.com)"}

print("Starting Producer: Streaming Wikimedia to Kafka...")

try:
    for event in EventSource(url, headers=headers):
        if event.event == "message":
            try:
                change = json.loads(event.data)

                # Send the whole dictionary to Kafka
                producer.send("wiki-changes", value=change)

                # Optional: visual feedback in terminal
                if change.get("server_name") == "en.wikipedia.org":
                    print(f"Sent to Kafka: {change['title']}")

            except (ValueError, KeyError):
                pass
except KeyboardInterrupt:
    print("Producer stopping...")
finally:
    producer.flush()
    producer.close()
