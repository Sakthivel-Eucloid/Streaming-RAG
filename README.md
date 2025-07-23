# Local RAG Chatbot with Streamlit, ChromaDB, and Ollama

This project provides a complete, containerized Retrieval-Augmented Generation (RAG) system. It ingests data from local JSON files, stores it in a Chroma vector database, and provides a Streamlit web UI to chat with the data using a locally-run Mistral-7B model via Ollama.

## 🧱 Architecture

1.  **Data Ingestion**: A one-time script (`data-loader`) reads all `.json` files from the `sample_data` directory.
2.  **Embedding**: The script uses a `sentence-transformers` model to compute vector embeddings for the content of each file.
3.  **Vector Storage**: The original data and its embedding are stored in a persistent ChromaDB instance.
4.  **LLM Service**: Ollama serves the `mistral:7b` language model locally.
5.  **UI**: A Streamlit application provides a chat interface. When a user asks a question:
    * The question is embedded.
    * A similarity search is performed on ChromaDB to find relevant documents (context).
    * The context and the original question are passed to the Ollama model to generate an answer.

All services are managed by Docker Compose.

## 🚀 How to Run

### Prerequisites
- [Docker](https://docs.docker.com/get-docker/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Steps

1.  **Create the Project Files**: Create all the directories and files as described in the project structure.

2.  **Start the Services**: Open a terminal in the project's root directory and run:

    ```bash
    docker-compose up --build -d
    ```
    * `--build`: This flag ensures Docker builds the images for the `data-loader` and `streamlit-app` services.
    * `-d`: This runs the containers in detached mode (in the background).

3.  **Wait for Initialization**: On the first run, be patient. The `ollama` service needs to download the `mistral:7b` model (which is several gigabytes). You can monitor the progress of the services:
    * Check all logs: `docker-compose logs -f`
    * Check a specific service: `docker-compose logs -f ollama`

4.  **Access the Chatbot**: Once all services are running (the `data-loader` will run and exit), open your web browser and navigate to:

    **http://localhost:8501**

5.  **Chat with Your Data**: You can now ask questions about the events in the `sample_data` directory. For example:
    * "What did user_alpha do?"
    * "Tell me about the add to cart event."

### 🧽 Shutting Down

To stop and remove all the containers, networks, and volumes created by the project, run:

```bash
docker-compose down -v
