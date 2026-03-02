import json
import time

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
from transformers import pipeline

# ─────────────────────────────────────────
# 1. Load DistilBERT Model (runs on CPU)
# ─────────────────────────────────────────
print("Loading AI model...")
classifier = pipeline(
    "zero-shot-classification",
    model="typeform/distilbert-base-uncased-mnli",  # lightweight zero-shot
)
LABELS = ["vandalism", "legitimate edit", "bot edit", "minor fix"]
print("Model loaded!")


# ─────────────────────────────────────────
# 2. Wait for Kafka to be ready
# ─────────────────────────────────────────
def wait_for_kafka(retries=10, delay=5):
    for attempt in range(retries):
        try:
            test_producer = KafkaProducer(bootstrap_servers="kafka:9092")
            test_producer.close()
            print("Kafka is ready!")
            return
        except NoBrokersAvailable:
            print(
                f"Kafka not ready, retrying in {delay}s... (attempt {attempt + 1}/{retries})"
            )
            time.sleep(delay)
    raise Exception("Could not connect to Kafka after multiple retries.")


wait_for_kafka()

# ─────────────────────────────────────────
# 3. Kafka Consumer (reads from producer)
# ─────────────────────────────────────────
consumer = KafkaConsumer(
    "wiki-changes",
    bootstrap_servers="kafka:9092",
    auto_offset_reset="latest",
    group_id="ai-processors",
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

# ─────────────────────────────────────────
# 4. Kafka Producer (sends results forward)
# ─────────────────────────────────────────
producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# ─────────────────────────────────────────
# 5. Main Processing Loop
# ─────────────────────────────────────────
print("Waiting for messages from Kafka...\n")

for message in consumer:
    change = message.value

    # Only process English Wikipedia edits
    if change.get("server_name") != "en.wikipedia.org":
        continue
    if change.get("type") != "edit":
        continue

    # Extract text for classification
    comment = change.get("comment", "").strip()
    title = change.get("title", "").strip()
    text = comment if comment else title

    if not text:
        continue

    try:
        # ── Run AI Classification ──
        result = classifier(text, LABELS, multi_label=False)
        top_label = result["labels"][0]
        confidence = round(result["scores"][0], 4)

        # ── Build enriched payload ──
        enriched = {
            "title": title,
            "user": change.get("user", "unknown"),
            "comment": comment,
            "ai_label": top_label,
            "ai_confidence": confidence,
            "is_bot": change.get("bot", False),
            "timestamp": change.get("timestamp"),
            "wiki_url": change.get("meta", {}).get("uri", ""),
        }

        # ── Send to next Kafka topic ──
        producer.send("wiki-classified", value=enriched)

        # ── Terminal feedback ──
        emoji = "🚨" if top_label == "vandalism" else "✅"
        print(
            f"{emoji} [{top_label.upper()}] ({confidence}) | {title} | by {enriched['user']}"
        )

    except Exception as e:
        print(f"Error processing message: {e}")
        continue
