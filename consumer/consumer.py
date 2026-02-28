import json

from kafka import KafkaConsumer

# 1. Setup Consumer
consumer = KafkaConsumer(
    "wiki-changes",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",  # Start from the beginning of the topic
    group_id="wiki-processors",  # Identifies this consumer group
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("Consumer started: Waiting for messages...")

for message in consumer:
    change = message.value

    # Process the data (Filtering logic happens here now!)
    if change.get("server_name") == "en.wikipedia.org":
        print(f"RECIEVED: Edit on {change['title']} by {change['user']}")
