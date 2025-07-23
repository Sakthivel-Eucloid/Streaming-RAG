import streamlit as st
import requests
import json
import os
import time
import chromadb
from sentence_transformers import SentenceTransformer

# --- Configuration ---
CHROMA_HOST = os.environ.get("CHROMA_HOST", "localhost")
CHROMA_PORT = int(os.environ.get("CHROMA_PORT", 8000))
OLLAMA_HOST = os.environ.get("OLLAMA_HOST", "localhost")
OLLAMA_BASE_URL = f"http://{OLLAMA_HOST}:11434"
EMBEDDING_MODEL = 'all-MiniLM-L6-v2'
LLM_MODEL = 'mistral:7b'
CHROMA_COLLECTION_NAME = "user_events"

# --- Helper Functions ---

@st.cache_resource
def get_chroma_client():
    """Get a ChromaDB client, retrying on failure."""
    # Implementation remains the same...
    while True:
        try:
            client = chromadb.HttpClient(host=CHROMA_HOST, port=CHROMA_PORT)
            client.heartbeat()
            return client
        except Exception as e:
            st.error(f"Could not connect to ChromaDB: {e}. Retrying in 5 seconds...")
            time.sleep(5)

@st.cache_resource
def get_embedding_model():
    """Load the sentence transformer model."""
    return SentenceTransformer(EMBEDDING_MODEL)

def wait_for_service(url, service_name):
    """Waits for a service to be ready by pinging its base URL."""
    while True:
        try:
            requests.get(url)
            st.session_state[f"{service_name}_ready"] = True
            return
        except requests.exceptions.ConnectionError:
            time.sleep(1)

def check_ollama_model(model_name):
    """Check if the specified model exists in Ollama."""
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags")
        response.raise_for_status()
        models = response.json().get("models", [])
        return any(model['name'] == model_name for model in models)
    except requests.exceptions.RequestException:
        return False

def find_relevant_context(query, collection, model, n_results=3):
    """Find relevant context in ChromaDB for a given query."""
    query_embedding = model.encode(query).tolist()
    results = collection.query(
        query_embeddings=[query_embedding],
        n_results=n_results
    )
    return results['documents'][0] if results['documents'] else []

def call_ollama(prompt):
    """Send a request to the Ollama model and stream the response."""
    try:
        payload = {
            "model": LLM_MODEL,
            "prompt": prompt,
            "stream": True
        }
        response = requests.post(f"{OLLAMA_BASE_URL}/api/generate", json=payload, stream=True)
        response.raise_for_status()
        
        full_response = ""
        for line in response.iter_lines():
            if line:
                try:
                    json_line = json.loads(line)
                    if 'response' in json_line:
                        full_response += json_line['response']
                        yield full_response
                except json.JSONDecodeError:
                    continue
    except requests.exceptions.RequestException as e:
        st.error(f"Error calling Ollama: {e}")
        yield "Sorry, I'm having trouble connecting to the language model."

# --- Streamlit UI ---
st.set_page_config(page_title="Chat with Your Data", layout="wide")
st.title("🤖 Chat with Your Event Data")

# --- Service Connection and Initialization ---
if 'services_ready' not in st.session_state:
    st.session_state.services_ready = False

if not st.session_state.services_ready:
    with st.status("Connecting to services...", expanded=True) as status:
        st.write("Connecting to Ollama...")
        wait_for_service(OLLAMA_BASE_URL, "ollama")
        status.update(label="Ollama connected.", state="running")
        
        st.write(f"Checking for LLM model: `{LLM_MODEL}`...")
        if check_ollama_model(LLM_MODEL):
            status.update(label=f"LLM model `{LLM_MODEL}` found.", state="running")
            st.session_state.model_ready = True
        else:
            status.update(label=f"LLM model `{LLM_MODEL}` not found in Ollama!", state="error", expanded=True)
            st.error(f"Ollama is running, but the '{LLM_MODEL}' model is not available.")
            st.info("Please pull the model by running this command in your terminal, then refresh this page:")
            st.code(f"docker run --rm -v {os.path.basename(os.getcwd())}_ollama-data:/root/.ollama ollama/ollama ollama pull {LLM_MODEL}", language="bash")
            st.stop()

        st.write("Connecting to ChromaDB...")
        chroma_client = get_chroma_client()
        status.update(label="ChromaDB connected.", state="running")
        
        st.write("Loading embedding model...")
        model = get_embedding_model()
        status.update(label="Embedding model loaded.", state="running")
        
        collection = chroma_client.get_collection(name=CHROMA_COLLECTION_NAME)
        status.update(label="All services ready!", state="complete", expanded=False)
        st.session_state.services_ready = True
else:
    chroma_client = get_chroma_client()
    model = get_embedding_model()
    collection = chroma_client.get_collection(name=CHROMA_COLLECTION_NAME)


st.markdown("This app uses a RAG pipeline to answer questions about user events from your JSON files.")

if "messages" not in st.session_state:
    st.session_state.messages = []

# Display chat messages
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask a question about user events"):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        message_placeholder = st.empty()
        
        with st.spinner("Finding relevant context..."):
            context_docs = find_relevant_context(prompt, collection, model)
        
        if not context_docs:
            st.warning("I couldn't find any relevant context for your query.")
            final_response = "I couldn't find any relevant context to answer your question."
            message_placeholder.markdown(final_response)
        else:
            context_str = "\n\n".join(context_docs)
            
            final_prompt = f"""
            Using the following context, please answer the user's question.
            If the context does not contain the answer, say that you don't have enough information.

            Context:
            ---
            {context_str}
            ---

            User Question: {prompt}
            """
            
            response_generator = call_ollama(final_prompt)
            final_response = ""
            for partial_response in response_generator:
                message_placeholder.markdown(partial_response + "▌")
                final_response = partial_response
            message_placeholder.markdown(final_response)

    st.session_state.messages.append({"role": "assistant", "content": final_response})

