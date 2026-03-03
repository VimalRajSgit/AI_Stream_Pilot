# 🧠 AI Stream Pilot

**Real-time Wikipedia edit classification using Kafka, AI, and Kubernetes.**

AI Stream Pilot streams live Wikipedia edits through Apache Kafka, classifies them with a DistilBERT zero-shot model, and auto-scales AI processing containers based on Kafka consumer lag.

---

## 📐 Architecture

```
Wikipedia SSE Stream
        │
        ▼
┌───────────────┐    Kafka: wiki-changes    ┌──────────────────┐    Kafka: wiki-classified    ┌──────────────┐
│   Producer    │ ─────────────────────────► │  AI Processor    │ ──────────────────────────► │   Consumer   │
│ (producer.py) │                            │(ai_processor.py) │                              │(consumer.py) │
└───────────────┘                            └──────────────────┘                              └──────────────┘
  Streams live                                Classifies edits                                  Reads enriched
  edits into Kafka                            using DistilBERT                                  results
                                                      ▲
                                                      │ scales
                                              ┌───────────────┐
                                              │ Auto-Scaler   │
                                              │(auto_scaller.py)│
                                              └───────────────┘
                                              Monitors Kafka lag
                                              & adjusts replicas
```

### Data Flow

1. **Producer** connects to the [Wikimedia EventStreams SSE API](https://stream.wikimedia.org/v2/stream/recentchange) and publishes every recent-change event to the Kafka topic `wiki-changes`.
2. **AI Processor** consumes from `wiki-changes`, runs zero-shot classification on each edit's comment/title, and publishes enriched results to `wiki-classified`.
3. **Consumer** reads from `wiki-classified` and prints the AI label, confidence score, article title, and editor.
4. **Auto-Scaler** continuously monitors the Kafka consumer lag for the `ai-processors` group and dynamically scales the AI Processor replicas up or down.

---

## 🧩 Project Structure

```
Kafka_kubernets_agents/
├── producer/
│   └── producer.py          # SSE → Kafka producer
├── consumer/
│   └── consumer.py          # Final classified-event consumer
├── ai_processor.py          # DistilBERT zero-shot classifier (core AI service)
├── auto_scaller.py          # Lag-based auto-scaler for AI Processor
├── wikipedia.py             # Standalone SSE stream test script
├── docker-compose.yml       # Full local orchestration (Kafka + Zookeeper + services)
├── dockerfile               # Docker image for services
├── requirements.txt         # Python dependencies
├── k8s/                     # Kubernetes manifests for cloud deployment
│   ├── apps.yaml            # Deployments for producer, consumer, AI processor
│   ├── kafka.yaml           # Kafka StatefulSet + Service
│   └── zookeeper.yaml       # Zookeeper Deployment + Service
├── .gitignore
└── readme.md
```

---

## 🤖 AI Classification

The AI Processor uses **DistilBERT** (`typeform/distilbert-base-uncased-mnli`) for zero-shot text classification. Each edit is classified into one of four labels:

| Label             | Description                            |
| ----------------- | -------------------------------------- |
| `vandalism`       | Destructive or malicious edits         |
| `legitimate edit` | Genuine content contributions          |
| `bot edit`        | Automated / bot-generated changes      |
| `minor fix`       | Typo corrections, formatting tweaks    |

### Enriched Output Payload

```json
{
  "title": "Artificial intelligence",
  "user": "EditorName",
  "comment": "Fixed citation formatting",
  "ai_label": "minor fix",
  "ai_confidence": 0.8231,
  "is_bot": false,
  "timestamp": 1709145600,
  "wiki_url": "https://en.wikipedia.org/wiki/Artificial_intelligence"
}
```

---

## ⚡ Auto-Scaler

`auto_scaller.py` is a lightweight, lag-based auto-scaler that monitors the Kafka consumer group (`ai-processors`) and dynamically adjusts the number of `ai_processor` replicas using `docker-compose --scale`.

### How It Works

1. Every **10 seconds**, the scaler queries the committed offsets vs. the latest offsets for all partitions of the `wiki-changes` topic.
2. The total **consumer lag** (unprocessed messages) is calculated.
3. Based on the lag, scaling decisions are made:

| Condition            | Action                          |
| -------------------- | ------------------------------- |
| Lag **> 100**        | Scale up by 1 (max 5 replicas) |
| Lag **< 10**         | Scale down by 1 (min 1 replica)|
| Otherwise            | No change                       |

This ensures that during high-traffic periods (e.g., breaking news on Wikipedia), additional AI Processor containers spin up to handle the load, and scale back down during quieter periods.

---

## 🛠️ Tech Stack

| Component         | Technology                                          |
| ----------------- | --------------------------------------------------- |
| Streaming Source   | [Wikimedia EventStreams](https://stream.wikimedia.org/) (SSE) |
| Message Broker     | [Apache Kafka](https://kafka.apache.org/)           |
| AI Model           | [DistilBERT MNLI](https://huggingface.co/typeform/distilbert-base-uncased-mnli) (Hugging Face) |
| Autoscaling        | Custom lag-based scaler (`auto_scaller.py`)         |
| Containerization   | Docker + Docker Compose                             |
| Orchestration      | Kubernetes (manifests provided in `k8s/`)           |
| Language           | Python 3                                            |

---

## 🚀 Getting Started (Local)

### Prerequisites

- Python 3.9+
- Apache Kafka running locally on `localhost:9092`
- pip

### Install Dependencies

```bash
pip install kafka-python sseclient-py transformers torch requests
```

### Run the Pipeline

Open **three separate terminals** and run the services in order:

```bash
# Terminal 1 — Start the Producer
python producer/producer.py

# Terminal 2 — Start the AI Processor
python ai_processor.py

# Terminal 3 — Start the Consumer
python consumer/consumer.py
```

The producer will stream live Wikipedia edits → the AI processor classifies them → the consumer prints results:

```
🚨 [VANDALISM] (0.7812) | Main Page | by 192.168.1.1
✅ [LEGITIMATE EDIT] (0.9134) | Quantum computing | by ScienceEditor
✅ [MINOR FIX] (0.8567) | Python (programming language) | by TypoBot
```

### Run with Docker Compose

To run the entire stack (Kafka, Zookeeper, and all services) with a single command:

```bash
docker-compose up --build
```

To also run the auto-scaler in a separate terminal:

```bash
python auto_scaller.py
```

---

## ☸️ Kubernetes Deployment

Kubernetes manifests are provided in the `k8s/` directory for deploying the full pipeline to a cloud cluster or Minikube.

> **Note:** The Kubernetes deployment was not fully tested end-to-end locally due to hardware constraints. The provided manifests (`apps.yaml`, `kafka.yaml`, `zookeeper.yaml`) are structured and ready to use — apply them to your cluster and adjust resource limits as needed.

```bash
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/apps.yaml
```

For production, consider adding **KEDA** for event-driven autoscaling based on Kafka consumer lag, which would replace the Docker-based `auto_scaller.py` with native Kubernetes scaling.

---

## 🗺️ Roadmap

- [x] **Core pipeline works locally** — Producer → AI Processor → Consumer
- [x] **AI classification** — DistilBERT zero-shot on live Wikipedia edits
- [x] **Auto-Scaler** — Lag-based scaling of AI Processor replicas
- [x] **Dockerized** — Dockerfile + Docker Compose for full stack
- [x] **Kubernetes manifests** — Provided in `k8s/` for cloud deployment
- [ ] **KEDA autoscaling** — Event-driven pod scaling on Kubernetes

---

## 📄 License

This project is open source — feel free to use, modify, and contribute.

---

> Built with ❤️ using Kafka, Hugging Face Transformers, and Kubernetes.
