# System Guardian 🚀
An AI-powered incident management platform designed to autonomously monitor, analyze, and suggest resolutions for on-call incidents. System Guardian integrates with tools like Slack, GitHub, Datadog, and more to provide **real-time insights** and **AI-driven remediation suggestions**.

## 🌟 Features
- **Real-time Incident Detection**: Ingests and processes events from **Slack, GitHub, Datadog, Jira**, and other sources.
- **AI-Powered Resolution Suggestions**: Uses **GPT-4 / Llama 3** and **retrieval-based search** (FAISS/Weaviate) to suggest fixes based on historical incidents.
- **Event-Driven Architecture**: Utilizes **Kafka / RabbitMQ** for reliable message streaming and processing.
- **Scalable & Modular**: Microservice-based structure with **FastAPI**, **PostgreSQL**, and **Elasticsearch**.
- **Monitoring & Logging**: Tracks incidents, logs, and system health using **Datadog & ELK stack**.

---

## 📁 Project Structure
```bash
system_guardian
├── conftest.py           # Fixtures for all tests
├── db                    # Database configurations and models
│   ├── dao               # Data Access Objects (Interacts with the database)
│   └── models            # ORM models for database tables
├── __main__.py           # Startup script (Launches FastAPI with Uvicorn)
├── services              # External service integrations (RabbitMQ, Kafka, Redis, etc.)
│   ├── kafka_producer.py  # Publishes events to Kafka
│   ├── kafka_consumer.py  # Processes incoming messages from Kafka
│   ├── slack_service.py   # Handles Slack event ingestion
│   ├── github_service.py  # Processes GitHub event hooks
│   ├── datadog_service.py # Handles Datadog alerts
│   └── ai_engine.py       # AI processing and resolution suggestions
├── settings.py           # Main project configuration (DB, API keys, environment variables)
├── static                # Static content (if needed)
├── tests                 # Unit and integration tests
└── web                   # Web server and API endpoints
    ├── api               # REST API handlers
    │   ├── router.py     # Main API router
    │   ├── ingest.py     # API for ingesting events from external sources
    │   ├── incidents.py  # Incident retrieval and analysis API
    │   ├── resolution.py # AI-based resolution generator API
    │   └── health.py     # System health check endpoint
    ├── application.py    # FastAPI application setup
    └── lifetime.py       # Startup and shutdown tasks
```


⸻

🚀 Getting Started

1️⃣ Installation

Clone the repository and set up the environment:

```bash
git clone https://github.com/your-repo/system_guardian.git
cd system_guardian
python -m venv venv
source venv/bin/activate  # For macOS/Linux
venv\Scripts\activate     # For Windows
pip install -r requirements.txt
```

2️⃣ Environment Configuration

Create a .env file and configure the necessary settings:

```bash
DATABASE_URL=postgresql://user:password@localhost:5432/system_guardian
KAFKA_BROKER=kafka://localhost:9092
RABBITMQ_URL=amqp://user:password@localhost:5672
OPENAI_API_KEY=your-openai-api-key
SLACK_BOT_TOKEN=your-slack-bot-token
GITHUB_WEBHOOK_SECRET=your-github-webhook-secret
DATADOG_API_KEY=your-datadog-api-key
```

3️⃣ Run the Application

Start the services and run the FastAPI server:

```bash
uvicorn web.application:app --host 0.0.0.0 --port 8000 --reload
```

Or use Docker Compose to spin up the full environment:

```bash
docker-compose up --build
```


⸻

## 🔥 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| /api/ingest/slack | POST | Ingests Slack messages for incident detection |
| /api/ingest/github | POST | Captures GitHub events (PRs, Issues, Deployments) |
| /api/ingest/datadog | POST | Processes Datadog alerts |
| /api/incidents | GET | Retrieves historical incidents |
| /api/resolution | POST | AI-driven resolution suggestions |
| /api/health | GET | Health check |



⸻

🛠️ Development

Testing

Run unit tests using pytest:

```bash
pytest tests/
```
Linting & Formatting

Ensure code consistency with black and flake8:

```bash
black .
flake8 .
```
