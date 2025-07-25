# This script runs the consumer service that bridges Kafka and ChromaDB.
# It listens for messages on a Kafka topic, generates vector embeddings,
# and indexes the data in ChromaDB for real-time semantic search.

import os
import json
import time
import logging
import chromadb
from kafka import KafkaConsumer
from sentence_transformers import SentenceTransformer

# --- Configuration from Environment Variables ---
# This allows us to configure the service through the docker-compose.yml file.
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "user-events")
CHROMA_HOST = os.environ.get("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.environ.get("CHROMA_PORT", 8000))
EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
CHROMA_COLLECTION_NAME = "user_events_vector_store"

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_kafka_consumer(bootstrap_servers, topic):
    """
    Establishes a connection to a Kafka topic and returns a consumer.
    Includes a retry loop to handle cases where the Kafka broker is not yet ready.
    """
    while True:
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest', # Start reading from the beginning of the topic
                enable_auto_commit=True,
                group_id='chroma-vector-consumer-group',
                # Deserialize JSON messages from Kafka
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logging.info(f"Successfully connected to Kafka topic '{topic}'")
            return consumer
        except Exception as e:
            logging.error(f"Could not connect to Kafka: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def get_chroma_client(host, port):
    """
    Establishes a connection to the ChromaDB server.
    Includes a retry loop to handle cases where the ChromaDB server is not yet ready.
    """
    while True:
        try:
            client = chromadb.HttpClient(host=host, port=port)
            client.heartbeat() # Ping the server to confirm connection
            logging.info(f"Successfully connected to ChromaDB at {host}:{port}")
            return client
        except Exception as e:
            logging.error(f"Could not connect to ChromaDB: {e}. Retrying in 5 seconds...")
            time.sleep(5)

def process_message(message, model, collection):
    """
    Takes a single message from Kafka, generates an embedding, and stores it in ChromaDB.
    """
    try:
        event_data = message.value
        doc_id = str(event_data.get("event_id"))
        if not doc_id:
            logging.warning(f"Skipping message with no event_id: {event_data}")
            return

        logging.info(f"Processing event: {doc_id}")

        # 1. Create a descriptive text string from the event data for embedding.
        # This natural language representation helps the model capture the event's meaning.
        text_to_embed = (
            f"User {event_data.get('user_id', 'unknown')} performed a "
            f"'{event_data.get('event_type', 'none')}' action on the page "
            f"{event_data.get('page_url', '/')}. The transaction amount was "
            f"${event_data.get('amount', 0)}."
        )
        
        # 2. Generate the vector embedding using the SentenceTransformer model.
        embedding = model.encode(text_to_embed).tolist()
        
        # 3. The original event data is stored as the document content.
        document = json.dumps(event_data)
        
        # 4. Create metadata for filtering and querying in ChromaDB.
        metadata = {
            "user_id": str(event_data.get("user_id", "")),
            "event_type": str(event_data.get("event_type", "")),
            "page_url": str(event_data.get("page_url", "")),
            "amount": float(event_data.get("amount", 0.0))
        }

        # 5. Use upsert to add the new document or update it if the ID already exists.
        collection.upsert(
            ids=[doc_id],
            embeddings=[embedding],
            documents=[document],
            metadatas=[metadata]
        )
        logging.info(f"Successfully indexed event {doc_id} in ChromaDB.")

    except json.JSONDecodeError:
        logging.error(f"Failed to decode JSON from message: {message.value}")
    except Exception as e:
        logging.error(f"An error occurred while processing event: {e}")

def main():
    """
    The main execution block for the service.
    Initializes connections and starts the message consumption loop.
    """
    logging.info("Starting Kafka to ChromaDB consumer service...")
    
    # Load the embedding model once on startup.
    model = SentenceTransformer(EMBEDDING_MODEL)
    logging.info(f"Embedding model '{EMBEDDING_MODEL}' loaded.")

    # Connect to ChromaDB and get or create the collection.
    chroma_client = get_chroma_client(CHROMA_HOST, CHROMA_PORT)
    collection = chroma_client.get_or_create_collection(name=CHROMA_COLLECTION_NAME)
    logging.info(f"ChromaDB collection '{CHROMA_COLLECTION_NAME}' is ready.")

    # Connect to Kafka.
    kafka_consumer = get_kafka_consumer(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    
    # Start the main loop to listen for and process messages from Kafka.
    logging.info("Waiting for messages from Kafka...")
    for message in kafka_consumer:
        process_message(message, model, collection)

if __name__ == "__main__":
    main()
