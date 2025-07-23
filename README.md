# Kafka to Flink Streaming Pipeline (Team-Aligned)

This project sets up a real-time data pipeline using Apache Kafka and Apache Flink, based on the team's standard `docker-compose.yml`.

## 🚀 How to Run

### Step 1: Start the Infrastructure

First, ensure no old containers are running, then start the pipeline.

```bash
# Stop any potentially conflicting containers
docker stop $(docker ps -q) && docker rm $(docker ps -a -q)

# Start all services
docker-compose up --build -d