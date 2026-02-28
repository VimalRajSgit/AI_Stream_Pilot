import json

from kafka import KafkaConsumer

# 1. Setup Consumer

consumer = KafkaConsumer(
    "wiki-classified",  # ← changed topic
    bootstrap_servers="localhost:9092",
    auto_offset_reset="latest",  # ← changed to latest
    group_id="wiki-final-consumers",  # ← changed group id
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

for message in consumer:
    change = message.value
    print(
        f"[{change['ai_label'].upper()}] ({change['ai_confidence']}) | {change['title']} | by {change['user']}"
    )
