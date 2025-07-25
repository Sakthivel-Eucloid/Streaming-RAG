# Kafka to ChromaDB Vector Pipeline

This project demonstrates a real-time pipeline that consumes events from Apache Kafka, generates vector embeddings, and indexes them in a ChromaDB vector store. This is the core data ingestion path for a streaming RAG system.

## 🚀 How to Run

### Step 1: Clean Up and Start the System

First, ensure no old containers are running. Then, start all the services.

```bash
# Stop any potentially conflicting containers
docker stop $(docker ps -q) && docker rm $(docker ps -a -q)

# Build and start all services
docker-compose up --build -d