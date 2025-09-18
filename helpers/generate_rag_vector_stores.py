from langchain_text_splitters import RecursiveJsonSplitter, MarkdownHeaderTextSplitter
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_community.embeddings import OCIGenAIEmbeddings
import configparser
import json

# Parse config file
config = configparser.ConfigParser()
config.read('../config.ini')

EMBED_MODEL_ID = config.get("OCI", "embed_model_id")
SERVICE_ENDPOINT = config.get("OCI", "service_endpoint")
COMPARTMENT_ID = config.get("OCI", "compartment_id")

embeddings = OCIGenAIEmbeddings(
    model_id=EMBED_MODEL_ID,
    service_endpoint=SERVICE_ENDPOINT,
    compartment_id=COMPARTMENT_ID
)

# Create JSON splitter
json_splitter = RecursiveJsonSplitter()

# db_metric_definitions.json
with open ("../rag/db_metric_definitions.json", "r") as file:
    data = json.load(file)
documents = json_splitter.create_documents(texts=data)
db_metric_vector_store = InMemoryVectorStore(embeddings)
db_metric_vector_store.add_documents(documents)
db_metric_vector_store.dump(f"../rag/db_metric_definitions_{EMBED_MODEL_ID}.pkl")

# cell_metric_definitions.json
with open ("../rag/cell_metric_definitions.json", "r") as file:
    data = json.load(file)
documents = json_splitter.create_documents(texts=data)
cell_metric_vector_store = InMemoryVectorStore(embeddings)
cell_metric_vector_store.add_documents(documents)
cell_metric_vector_store.dump(f"../rag/cell_metric_definitions_{EMBED_MODEL_ID}.pkl")

# Create Markdown splitter
headers_to_split_on = [("Help for", "command")]
markdown_splitter = MarkdownHeaderTextSplitter(headers_to_split_on)

# dbmcli_help.pkl
with open("../rag/dbmcli_help.txt", "r") as file:
    data = file.read()
documents = markdown_splitter.split_text(data)
dbmcli_help_vector_store = InMemoryVectorStore(embeddings)
dbmcli_help_vector_store.add_documents(documents)
dbmcli_help_vector_store.dump(f"../rag/dbmcli_help_{EMBED_MODEL_ID}.pkl")

# cellcli_help.pkl
with open("../rag/cellcli_help.txt", "r") as file:
    data = file.read()
documents = markdown_splitter.split_text(data)
cellcli_help_vector_store = InMemoryVectorStore(embeddings)
cellcli_help_vector_store.add_documents(documents)
cellcli_help_vector_store.dump(f"../rag/cellcli_help_{EMBED_MODEL_ID}.pkl")

# Create Markdown splitter
headers_to_split_on = [("Attributes for", "object")]
markdown_splitter = MarkdownHeaderTextSplitter(headers_to_split_on)

# dbmcli_describe.pkl
with open("../rag/dbmcli_describe.txt", "r") as file:
    data = file.read()
documents = markdown_splitter.split_text(data)
dbmcli_describe_vector_store = InMemoryVectorStore(embeddings)
dbmcli_describe_vector_store.add_documents(documents)
dbmcli_describe_vector_store.dump(f"../rag/dbmcli_describe_{EMBED_MODEL_ID}.pkl")

# cellcli_describe.pkl
with open("../rag/cellcli_describe.txt", "r") as file:
    data = file.read()
documents = markdown_splitter.split_text(data)
cellcli_describe_vector_store = InMemoryVectorStore(embeddings)
cellcli_describe_vector_store.add_documents(documents)
cellcli_describe_vector_store.dump(f"../rag/cellcli_describe_{EMBED_MODEL_ID}.pkl")