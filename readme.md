# 🧠 AI Stream Pilot

**Real-time Wikipedia edit classification using Kafka, AI, and Kubernetes.**

AI Stream Pilot streams live Wikipedia edits through Apache Kafka, classifies them with a DistilBERT zero-shot model, and is designed to auto-scale AI processing pods via KEDA based on edit complexity.

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
```

### Data Flow

1. **Producer** connects to the [Wikimedia EventStreams SSE API](https://stream.wikimedia.org/v2/stream/recentchange) and publishes every recent-change event to the Kafka topic `wiki-changes`.
2. **AI Processor** consumes from `wiki-changes`, runs zero-shot classification on each edit's comment/title, and publishes enriched results to `wiki-classified`.
3. **Consumer** reads from `wiki-classified` and prints the AI label, confidence score, article title, and editor.

---

## 🧩 Project Structure

```
Kafka_kubernets_agents/
├── producer/
│   └── producer.py          # SSE → Kafka producer
├── consumer/
│   └── consumer.py          # Final classified-event consumer
├── ai_processor.py          # DistilBERT zero-shot classifier (core AI service)
├── wikipedia.py             # Standalone SSE stream test script
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

## 🛠️ Tech Stack

| Component         | Technology                                          |
| ----------------- | --------------------------------------------------- |
| Streaming Source   | [Wikimedia EventStreams](https://stream.wikimedia.org/) (SSE) |
| Message Broker     | [Apache Kafka](https://kafka.apache.org/)           |
| AI Model           | [DistilBERT MNLI](https://huggingface.co/typeform/distilbert-base-uncased-mnli) (Hugging Face) |
| Language           | Python 3                                            |
| Orchestration      | Kubernetes (Minikube) — *coming soon*               |
| Autoscaling        | KEDA — *coming soon*                                |

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

---

## 🗺️ Roadmap

- [x] **Code works locally** ← *you are here*
- [ ] **Dockerize** — Create Dockerfiles for each service
- [ ] **Docker Compose** — Orchestrate all 3 services + Kafka + Zookeeper
- [ ] **Deploy to Minikube** — Kubernetes manifests for local cluster
- [ ] **Add KEDA autoscaling** — Scale AI processor pods based on Kafka consumer lag & edit complexity

### KEDA Vision

The end goal is to dynamically scale the AI Processor pods based on:
- **Kafka consumer lag** — more unprocessed messages → more pods
- **Edit complexity** — heavier classification workloads trigger scaling

```
                    KEDA ScaledObject
                          │
                          ▼
              ┌───────────────────────┐
              │   Kafka Lag Trigger    │
              │  (wiki-changes topic)  │
              └───────┬───────────────┘
                      │ scales
                      ▼
         ┌─────────────────────────┐
         │  AI Processor Pods (n)  │
         │  DistilBERT classifiers │
         └─────────────────────────┘
```

---

## 📄 License

This project is open source — feel free to use, modify, and contribute.

---

> Built with ❤️ using Kafka, Hugging Face Transformers, and Kubernetes.
