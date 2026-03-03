import subprocess
import time

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
KAFKA_BROKER = "localhost:9092"
TOPIC = "wiki-changes"
GROUP_ID = "ai-processors"
MIN_REPLICAS = 1
MAX_REPLICAS = 5
SCALE_UP_THRESHOLD = 100  # lag > 100 → scale up
SCALE_DN_THRESHOLD = 10  # lag < 10  → scale down
CHECK_INTERVAL = 10  # check every 10 seconds

current_replicas = 1


# ─────────────────────────────────────────
# Get Kafka Lag
# ─────────────────────────────────────────
def get_kafka_lag():
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BROKER,
            group_id=GROUP_ID,
            enable_auto_commit=False,
        )
        partitions = consumer.partitions_for_topic(TOPIC)
        if not partitions:
            consumer.close()
            return 0

        from kafka import TopicPartition

        tps = [TopicPartition(TOPIC, p) for p in partitions]
        consumer.assign(tps)
        consumer.seek_to_end(*tps)
        end_offsets = {tp: consumer.position(tp) for tp in tps}

        consumer.seek_to_beginning(*tps)
        committed = {}
        for tp in tps:
            committed[tp] = consumer.committed(tp) or 0

        lag = sum(end_offsets[tp] - committed[tp] for tp in tps)
        consumer.close()
        return lag
    except Exception as e:
        print(f"Error getting lag: {e}")
        return 0


# ─────────────────────────────────────────
# Scale ai_processor
# ─────────────────────────────────────────
def scale(replicas):
    global current_replicas
    if replicas == current_replicas:
        return
    print(f"⚡ Scaling ai_processor: {current_replicas} → {replicas} replicas")
    subprocess.run(
        ["docker-compose", "up", "--scale", f"ai_processor={replicas}", "-d"],
        check=True,
    )
    current_replicas = replicas


# ─────────────────────────────────────────
# Main Loop
# ─────────────────────────────────────────
print("🚀 AutoScaler started — watching Kafka lag...\n")

while True:
    lag = get_kafka_lag()
    print(f"📊 Kafka lag: {lag} | Current replicas: {current_replicas}")

    if lag > SCALE_UP_THRESHOLD and current_replicas < MAX_REPLICAS:
        scale(min(current_replicas + 1, MAX_REPLICAS))

    elif lag < SCALE_DN_THRESHOLD and current_replicas > MIN_REPLICAS:
        scale(max(current_replicas - 1, MIN_REPLICAS))

    else:
        print(f"✅ No scaling needed")

    time.sleep(CHECK_INTERVAL)
