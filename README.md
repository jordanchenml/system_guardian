# System Guardian 🚀
An AI-powered incident management platform designed to autonomously monitor, analyze, and suggest resolutions for on-call incidents. System Guardian integrates with tools like Slack, GitHub, Datadog, and more to provide **real-time insights** and **AI-driven remediation suggestions**.

## 🌟 Features
- **Real-time Incident Detection**: Ingests and processes events from **Slack, GitHub, Jira**, and other sources.
- **AI-Powered Resolution Suggestions**: Uses **GPT-4 / Llama 3** and **retrieval-based search** (Qdrant) to suggest fixes based on historical incidents.
- **Event-Driven Architecture**: Utilizes **RabbitMQ** for reliable message streaming and processing.
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
├── services              # External service integrations and services
│   ├── ai                # AI-related services
│   │   ├── incident_similarity.py  # Finding similar incidents with embeddings
│   ├── vector_db         # Vector database services
│   │   ├── qdrant_client.py        # Qdrant client for vector storage
│   │   ├── dependencies.py         # Dependency injection for Qdrant
│   ├── ingest            # Data ingestion services
│   │   ├── message_publisher.py    # Service for publishing messages
│   ├── rabbit            # RabbitMQ integration
├── settings.py           # Main project configuration (DB, API keys, environment variables)
├── static                # Static content (if needed)
├── tests                 # Unit and integration tests
└── web                   # Web server and API endpoints
    ├── api               # REST API handlers
    │   ├── router.py     # Main API router
    │   ├── ingest        # API for ingesting events from external sources
    │   │   ├── github    # GitHub webhook handlers
    │   │   ├── jira      # Jira webhook handlers
    │   ├── incidents     # Incident retrieval and analysis API
    │   │   ├── schema.py # Incident API schemas
    │   │   ├── views.py  # Incident API endpoint handlers
    │   ├── vector_db     # Vector database API
    │   │   ├── schema.py # Vector DB API schemas
    │   │   ├── views.py  # Vector DB API endpoint handlers
    │   ├── monitoring    # System monitoring endpoints
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
RABBITMQ_URL=amqp://user:password@localhost:5672
OPENAI_API_KEY=your-openai-api-key
SLACK_BOT_TOKEN=your-slack-bot-token
GITHUB_WEBHOOK_SECRET=your-github-webhook-secret
DATADOG_API_KEY=your-datadog-api-key
QDRANT_HOST=localhost
QDRANT_PORT=6333
```

3️⃣ Run the Application

Start the services and run the FastAPI server:

```bash
poetry run python -m system_guardian
```

Or use Docker Compose to spin up the full environment:

```bash
docker-compose up --build
```


⸻

## 📊 Visualization Interfaces

System Guardian provides several visualization interfaces to help you monitor and manage various components of the system:

### Qdrant Vector Database Dashboard

Qdrant offers an intuitive web interface for managing and monitoring vector collections:

- **Access URL**: [http://localhost:6333/dashboard](http://localhost:6333/dashboard)
- **Features**:
  - View and manage vector collections
  - Monitor collection statistics
  - Execute vector searches and test queries
  - View collection configurations and index settings

### RabbitMQ Management Interface

The RabbitMQ management interface allows you to monitor and manage the message queuing system:

- **Access URL**: [http://localhost:15672](http://localhost:15672)
- **Default Credentials**:
  - Username: guest
  - Password: guest
- **Features**:
  - Monitor queue status and message traffic
  - View exchanges and binding configurations
  - Publish and receive test messages
  - Manage users and permissions
  - View performance metrics and system resource usage

These visualization interfaces greatly simplify the development and debugging process, allowing you to intuitively understand the operational status of the system.

⸻

## 🔥 API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| /api/ingest/slack | POST | Ingests Slack messages for incident detection |
| /api/ingest/github | POST | Captures GitHub events (PRs, Issues, Deployments) |
| /api/ingest/datadog | POST | Processes Datadog alerts |
| /api/ingest/jira | POST | Captures Jira events (Issues, Comments) |
| /api/incidents/similar | POST | Find incidents similar to a query text |
| /api/incidents/index | POST | Index an incident for similarity search |
| /api/vector-db/collections | GET | List all vector collections |
| /api/vector-db/collections/{name} | GET | Get details of a specific collection |
| /api/vector-db/collections/{name} | POST | Create a new vector collection |
| /api/monitoring/health | GET | Health check endpoint to verify system status |
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


