import os
import json
import time
import logging
import chromadb
from sentence_transformers import SentenceTransformer

# --- Configuration ---
CHROMA_HOST = os.environ.get("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.environ.get("CHROMA_PORT", 8000))
EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
CHROMA_COLLECTION_NAME = "user_events"
DATA_DIR = "/app/sample_data"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_chroma_client(host, port):
    """Connects to ChromaDB with retries."""
    while True:
        try:
            client = chromadb.HttpClient(host=host, port=port)
            client.heartbeat()
            logging.info(f"Successfully connected to ChromaDB at {host}:{port}")
            return client
        except Exception as e:
            logging.error(f"Could not connect to ChromaDB: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def main():
    """Reads JSON files, creates embeddings, and stores them in ChromaDB."""
    logging.info("Starting data ingestion process...")

    chroma_client = get_chroma_client(CHROMA_HOST, CHROMA_PORT)
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    collection = chroma_client.get_or_create_collection(
        name=CHROMA_COLLECTION_NAME
    )

    processed_files = 0
    for filename in os.listdir(DATA_DIR):
        if filename.endswith(".json"):
            filepath = os.path.join(DATA_DIR, filename)
            try:
                with open(filepath, 'r') as f:
                    event_data = json.load(f)
                
                doc_id = str(event_data.get("event_id"))

                # Check if the document already exists
                if collection.get(ids=[doc_id])['ids']:
                    logging.info(f"Event {doc_id} already exists in ChromaDB. Skipping.")
                    continue

                # Create a text representation for embedding
                text_to_embed = f"Event type: {event_data.get('event_type', '')}. Page URL: {event_data.get('page_url', '')}. User: {event_data.get('user_id', '')}"
                
                embedding = model.encode(text_to_embed).tolist()
                document = json.dumps(event_data)
                metadata = {
                    "user_id": str(event_data.get("user_id", "")),
                    "event_type": str(event_data.get("event_type", "")),
                    "page_url": str(event_data.get("page_url", ""))
                }

                collection.add(
                    ids=[doc_id],
                    embeddings=[embedding],
                    documents=[document],
                    metadatas=[metadata]
                )
                logging.info(f"Successfully ingested event {doc_id} from {filename}")
                processed_files += 1

            except Exception as e:
                logging.error(f"Failed to process {filename}: {e}")

    logging.info(f"Data ingestion complete. Processed {processed_files} new files.")

if __name__ == "__main__":
    main()
