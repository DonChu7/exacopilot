import sys
import asyncio
import configparser
from termcolor import colored
import argparse
from datetime import datetime
from zoneinfo import ZoneInfo
import multiprocessing
import traceback
import contractions
import uuid
import os

# FastMCP imports
from fastmcp import Client
from fastmcp.client.sampling import (
    SamplingMessage,
    SamplingParams,
    RequestContext
)

# Agentic AI imports
from langchain_community.chat_models.oci_generative_ai import ChatOCIGenAI
from langchain_community.embeddings import OCIGenAIEmbeddings
from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage, AnyMessage
from langchain_mcp_adapters.tools import load_mcp_tools
from langgraph.prebuilt import create_react_agent
from langgraph.prebuilt.chat_agent_executor import AgentState

# Short-term memory imports
from langgraph.checkpoint.memory import InMemorySaver
from langchain_core.messages.utils import trim_messages

# Long-term memory imports
import oracledb
from langchain_community.vectorstores.oraclevs import OracleVS
from langchain_core.vectorstores import InMemoryVectorStore
from langchain_core.documents import Document

# Custom imports
import workarounds
from server import get_query, set_response, polling_supported

# =====
# Setup
# =====

# Parse args
parser = argparse.ArgumentParser()
parser.add_argument('--verbose', '-v', action='store_true', help="Print agent steps for debugging.")
parser.add_argument('--poll', '-p', action='store_true', help="Run polling in addition to interactive chat.")
parser.add_argument('--database', '-d', action='store_true', help="Store long-term memory in Oracle Database.")
parser.add_argument('--read-only', '-r', action='store_true', help="Run with read-only tools.")
args = parser.parse_args()

# Read config file
config = configparser.ConfigParser()
config.read('config.ini')

# Parse OCI config fields
CHAT_MODEL_ID = config.get("OCI", "chat_model_id")
EMBED_MODEL_ID = config.get("OCI", "embed_model_id")
SAMPLING_MODEL_ID = config.get("OCI", "sampling_model_id")
SERVICE_ENDPOINT = config.get("OCI", "service_endpoint")
COMPARTMENT_ID = config.get("OCI", "compartment_id")

# Parse system config fields
USERNAME = config.get("SYSTEM", "username")
TZ_IDENTIFIER = config.get("SYSTEM", "tz_identifier")
DB_NODES = [node.strip() for node in config.get("SYSTEM", "db_nodes").split(',')]
CELL_NODES = [node.strip() for node in config.get("SYSTEM", "cell_nodes").split(',')]
DCLI_PATH = config.get("SYSTEM", "dcli_path")
EXADATA_QA_PATH = config.get("SYSTEM", "exadata_qa_path")

# Parse database config fields
CONNECTION_STRING = config.get("DB", "connection_string")
WALLET_PATH = config.get("DB", "wallet_path")
CLIENT_LIBRARY_PATH = config.get("DB", "client_library_path")

# Check that required fields are filled
if not all(
    [
        CHAT_MODEL_ID,
        EMBED_MODEL_ID,
        SERVICE_ENDPOINT,
        COMPARTMENT_ID
    ]
):
    print(colored("Error: Missing OCI information in config.ini.", "red"))
    sys.exit(0)
if not SAMPLING_MODEL_ID:
    SAMPLING_MODEL_ID = CHAT_MODEL_ID
if not all(
    [
        USERNAME,
        TZ_IDENTIFIER,
        DB_NODES,
        CELL_NODES,
        DCLI_PATH
    ]
):
    print(colored("Error: Missing SYSTEM information in config.ini.", "red"))
    sys.exit(0)
if args.poll and not all(
    [
        EXADATA_QA_PATH
    ]
):
    print(colored("Error: Missing SYSTEM information in config.ini.", "red"))
    sys.exit(0)
if args.database and not all(
    [
        CONNECTION_STRING,
        WALLET_PATH
    ]
):
    print(colored("Error: Missing DB information in config.ini.", "red"))
    sys.exit(0)

# Initialize chat model
chat_model_provider = "cohere"
if not CHAT_MODEL_ID.startswith("cohere"):
    chat_model_provider = "meta"

chat_model = ChatOCIGenAI(
    model_id=CHAT_MODEL_ID,
    service_endpoint=SERVICE_ENDPOINT,
    compartment_id=COMPARTMENT_ID,
    model_kwargs={
        "temperature": 0.3,
        "max_tokens": 4000,
    },
    provider=chat_model_provider
)

# Initialize embed model
embed_model = OCIGenAIEmbeddings(
    model_id=EMBED_MODEL_ID,
    service_endpoint=SERVICE_ENDPOINT,
    compartment_id=COMPARTMENT_ID,
)

# Initialize sampling model
sampling_model_provider = "cohere"
if not SAMPLING_MODEL_ID.startswith("cohere"):
    sampling_model_provider = "meta"

sampling_model = ChatOCIGenAI(
    model_id=SAMPLING_MODEL_ID,
    service_endpoint=SERVICE_ENDPOINT,
    compartment_id=COMPARTMENT_ID,
    model_kwargs={
        "temperature": 0,
        "max_tokens": 4000,
    },
    provider=sampling_model_provider
)

def get_current_time() -> str:
    """
    Get the current date and time in ISO 8601 format.
    This tool can help address queries involving a time relative to the current time.
    """
    utc_now = datetime.now(ZoneInfo(TZ_IDENTIFIER))
    local_now = utc_now.astimezone()
    return local_now.isoformat(timespec='seconds')

def get_utc_offset_hours() -> float:
    """
    Get hours a given time zone is offset from UTC.
    This information helps ExaCopilot convert user-specified dates and times to ISO 8601 format.
    """
    tz = ZoneInfo(TZ_IDENTIFIER)
    utc_now = datetime.now(ZoneInfo('UTC'))
    offset = tz.utcoffset(utc_now)
    if offset is not None:
        return offset.total_seconds() / 3600
    else:
        return 0

def chat_recurring_prompt(state: AgentState) -> list[AnyMessage]:
    # Get relevant episodes
    episode_info = ""
    if args.database:
        episodes = episodic_memory.similarity_search(query=state["messages"][-1].content, k=3, filter={"username": USERNAME})
    else:
        episodes = episodic_memory.similarity_search(query=state["messages"][-1].content, k=3)
    if episodes: 
        episode_info = "Below are episodes that describe how you responded to past queries. To find out about the current state of your fleet, do not rely on these episodes; execute the appropriate tool."
        for episode in episodes:
            episode_info += f"\n\nEpisode: {episode.page_content}"
    # Get relevant memories
    memory_info = ""
    if args.database:
        memories = semantic_memory.similarity_search(query=state["messages"][-1].content, k=3, filter={"username": USERNAME})
    else:
        memories = semantic_memory.similarity_search(query=state["messages"][-1].content, k=3)
    if memories:
        memory_info = "Below are memories that you have saved from the user."
        for memory in memories:
            memory_info += f"\n\nMemory: {memory.page_content}"
    # Construct prompt
    prompt = f"""
    You are ExaCopilot, an assistant designed to help a user administer an Exadata system.
    The user's name is {USERNAME}. The user's fleet includes the following database (DB) nodes/servers: {", ".join(DB_NODES)}. The user's fleet includes the following cell/storage nodes/servers: {", ".join(CELL_NODES)}.
    The nodes in the user's fleet are the only nodes you have access to. You can respond to queries about Exadata and Oracle, using tools as you see fit. When describing a long tool output, summarize concisely.
    Only use a tool if it makes sense to do so in response to a query. If you cannot complete a request, say so.
    Make sure you provide all of the appropriate arguments when calling tools. Do not call tools without the appropriate arguments.

    Tool selection guidance:
    - Prefer the tool `rag_cot_search` for step-by-step how-tos, comprehensive guides, procedures, or troubleshooting requests (e.g., "how to", "guide", "document", "troubleshoot", "steps").
    - Prefer the tool `rag_standard_search` for quick fact lookup, short answers, citations, or simple one-off questions.
    - When in doubt between the two, choose `rag_cot_search` if the user asks for a document, plan, or multiple operations; choose `rag_standard_search` for simple lookups.
    The time zone is {TZ_IDENTIFIER}, which is offset from UTC by {get_utc_offset_hours()}. The current time is {get_current_time()}. Keep the time in mind when executing tools.
    
    If the user asks about conversation history, respond to the best of your ability. If appropriate, let the user know you only have access to recent messages and relevant memories.
    If the user asks you to save a memory or remember something, save it to your long-term memory using the save_memory tool.
    If the user asks you to delete a memory or forget something, remove it from your long-term memory using the delete_memory tool.
    Do not use tools to recall memories; relevant memories are automatically provided to you.

    {memory_info}

    {episode_info}
    """
    return [{"role": "system", "content": prompt}] + state["messages"]

def poll_recurring_prompt(state: AgentState) -> list[AnyMessage]:
    # Construct prompt
    prompt = f"""
    You are ExaCopilot, an assistant designed to respond to queries about Exadata and a given fleet.
    The fleet includes the following database (DB) nodes/servers: {", ".join(DB_NODES)}. The fleet includes the following cell/storage nodes/servers: {", ".join(CELL_NODES)}.
    The nodes in the fleet are the only nodes you have access to. You can respond to queries about Exadata and Oracle, using tools as you see fit.
    Only use a tool if it makes sense to do so in response to a query. If you cannot complete a request, say so.
    Make sure you provide all of the appropriate arguments when calling tools. Do not call tools without the appropriate arguments.
    The time zone is {TZ_IDENTIFIER}, which is offset from UTC by {get_utc_offset_hours()}. The current time is {get_current_time()}. Keep the time in mind when executing tools.
    """
    return [{"role": "system", "content": prompt}] + state["messages"]

async def sampling_handler(
    messages: list[SamplingMessage],
    params: SamplingParams,
    context: RequestContext
) -> str:
    """
    Define how sampling is handled.
    """
    # Extract message content
    conversation = []
    for message in messages:
        content = message.content.text if hasattr(message.content, 'text') else str(message.content)
        conversation.append(f"{message.role}: {content}")
    response = sampling_model.invoke(conversation)
    return response.content

SERVER = "server.py"
if args.read_only:
    SERVER = "server_read_only.py"

# Placeholder for SQLcl integration
SQLCL_PATH = ""

mcp_config = {
    "mcpServers": {
        "exadata": {
            "transport": "stdio",
            "command": "python",
            "args": [f"./{SERVER}"]
        },
        "rag": {
            "transport": "stdio",
            "command": "python",
            "args": ["./server_rag.py"]
        },
        # "sqlcl": {
        #     "command": os.path.join(SQLCL_PATH, "bin/sql"),
        #     "args": ["-mcp"]
        # }
    }
}

# Initialize MCP client
client = Client(
    mcp_config,
    sampling_handler=sampling_handler
)

# ==========
# Chat agent
# ==========

# Add checkpointer for short-term memory
checkpointer = InMemorySaver()

# Trim short-term memory messages to the 10 most recent ones before each LLM call
def pre_model_hook(state):
    messages = state["messages"]
    trimmed_messages = trim_messages(
        messages,
        strategy="last",
        token_counter=len,
        max_tokens=10,
        start_on=("system", "human"),
        end_on=("tool", "human")
    )
    return {"llm_input_messages": trimmed_messages}

# Load long-term memory
if args.database:
    # Initialize client in thick mode
    if sys.platform == "win32" or sys.platform == "darwin":
        # Windows or macOS
        try:
            oracledb.init_oracle_client(lib_dir=CLIENT_LIBRARY_PATH)
        except:
            print(colored("Error: Oracle Client library could not be loaded. Please specify a valid client_library_path in config.ini.", "red"))
            sys.exit(0)
    else:
        # Linux
        try:
            oracledb.init_oracle_client()
        except:
            print(colored("Error: Oracle Client library could not be loaded. Please set the LD_LIBRARY_PATH environment variable to a valid Oracle Client library directory.", "red"))
            sys.exit(0)
    conn = oracledb.connect(externalauth=True, wallet_location=WALLET_PATH, dsn=CONNECTION_STRING)
    # Load episodic long-term memory
    episodic_memory = OracleVS(
        client=conn,
        table_name="exacopilot_episodic_memory",
        embedding_function=embed_model
    )
    # Load semantic long-term memory
    semantic_memory = OracleVS(
        client=conn,
        table_name="exacopilot_semantic_memory",
        embedding_function=embed_model
    )
else:
    # Load episodic long-term memory
    if os.path.exists(f"memory/episodic_memory_{USERNAME.lower()}.pkl"):
        episodic_memory = InMemoryVectorStore.load(f"memory/episodic_memory_{USERNAME.lower()}.pkl", embedding=embed_model)
    else:
        episodic_memory = InMemoryVectorStore(embedding=embed_model)
    # Load semantic long-term memory
    if os.path.exists(f"memory/semantic_memory_{USERNAME.lower()}.pkl"):
        semantic_memory = InMemoryVectorStore.load(f"memory/semantic_memory_{USERNAME.lower()}.pkl", embedding=embed_model)
    else:
        semantic_memory = InMemoryVectorStore(embedding=embed_model)

# Save an episode to long-term memory
async def save_episode(response: str):
    """
    Analyze and save an agent episode.
    """
    prompt = f"""
    Look at the sequence of actions below. Was it successful or useful to the user?
    If so, please summarize this sequence of actions from the perspective of the agent in 2-5 sentences.
    If not, output only the sentence: "Unsuccessful episode."

    {response}
    """
    summary = await sampling_model.ainvoke(prompt)
    if summary.content == "Unsuccessful episode.":
        return
    id = str(uuid.uuid4())
    document = Document(id=id, page_content=summary.content, metadata={"username": USERNAME, "id": id})
    # Save episode
    episodic_memory.add_documents(documents=[document])

# Tool to save a memory (separate from MCP tools but accessed in the same way)
async def save_memory(memory: str) -> str:
    """
    Save something the user said to your memory.
    
    Args:
        memory (str): The memory to be saved.
    
    Returns:
        str: ID of the memory saved.
    """
    id = str(uuid.uuid4())
    document = Document(id=id, page_content=memory, metadata={"username": USERNAME, "id": id})
    # Save memory
    semantic_memory.add_documents(documents=[document])
    return "Successfully saved memory."

# Tool to delete a memory (separate from MCP tools but accessed in the same way)
async def delete_memory(memory: str) -> str:
    """
    Delete something the user said from your memory.
    
    Args:
        memory (str): The memory to be deleted.
    
    Returns:
        str: ID of the memory deleted.
    """
    if args.database:
        docs = semantic_memory.similarity_search(query=memory, k=3, filter={"username": USERNAME})
    else:
        docs = semantic_memory.similarity_search(query=memory, k=3)
    old_memories = "\n".join([f"ID: {doc.metadata['id']}\nMemory: {doc.page_content}" for doc in docs])
    prompt = f"""
    You will receive the description of a memory the user wants to delete, along with several candidate memories.
    If the description fits one of the candidate memories, output only the ID of the best memory to delete.
    Otherwise, output only the sentence: "Error: No memory found."

    Description: {memory}

    Candidate memories:
    {old_memories}
    """
    llm_output = await sampling_model.ainvoke(prompt)
    if llm_output.content == "Error: No memory found.":
        return llm_output.content
    id = llm_output.content
    # Delete memory
    semantic_memory.delete(ids=[id])
    return "Successfully deleted memory."

# Chat loop
async def chat(poll_proc, exit_event):
    """
    Chat loop for agent to interact with user.
    """
    async with client:
        tools = await load_mcp_tools(client.session)
        # Ensure every tool has a non-empty description to satisfy OCI tool schema
        def _ensure_tool_descriptions(ts):
            sanitized = []
            for t in ts:
                try:
                    desc = getattr(t, "description", None)
                    name = getattr(t, "name", t.__class__.__name__)
                    if not desc or not str(desc).strip():
                        setattr(t, "description", f"{name} tool")
                except Exception:
                    pass
                sanitized.append(t)
            return sanitized
        tools = _ensure_tool_descriptions(tools)
        # Create agent
        agent = create_react_agent(
            model=chat_model,
            tools=tools + [save_memory, delete_memory],
            checkpointer=checkpointer,
            prompt=chat_recurring_prompt,
            pre_model_hook=pre_model_hook,
        )
        config = {"configurable": {"thread_id": "1"}}
        # Initial message
        print(colored("ExaCopilot: ", "light_magenta"), end="")
        chat_initial_prompt = "Welcome the user and introduce yourself. Let the user know what database and cell nodes you have access to."
        if args.verbose:
            # Verbose mode
            async for chunk in agent.astream(
                {"messages": [SystemMessage(chat_initial_prompt)]},
                stream_mode="debug",
                config=config
            ):
                print(chunk)
            print("")
        else:
            # Default mode
            async for chunk in agent.astream(
                {"messages": [SystemMessage(chat_initial_prompt)]},
                stream_mode="messages",
                config=config
            ):
                print(chunk[0].content, end="")
            print("\n")
        while True:
            try:
                query = input(colored("You: ", "light_cyan"))
                if len(query) == 0:
                    continue
                elif query.lower() == 'quit':
                    # Quit ExaCopilot
                    if args.database:
                        conn.close()
                    else:
                        episodic_memory.dump(f"memory/episodic_memory_{USERNAME.lower()}.pkl")
                        semantic_memory.dump(f"memory/semantic_memory_{USERNAME.lower()}.pkl")
                    if poll_proc:
                        exit_event.set()
                        print(colored("\nWaiting for polling of current node to finish...", "yellow"))
                    sys.exit(0)
                print(colored("\nExaCopilot: ", "light_magenta"), end="")
                if args.verbose:
                    # Verbose mode
                    response = []
                    tools_in_response = False
                    async for stream_mode, chunk in agent.astream(
                        {"messages": [HumanMessage(content=query)]},
                        stream_mode=["debug", "updates"],
                        config=config
                    ):
                        if stream_mode == "debug":
                            print(chunk)
                        else:
                            # Construct episode
                            if "pre_model_hook" not in chunk:
                                response.append(chunk)
                            if "tools" in chunk:
                                tools_in_response = True             
                    if tools_in_response:
                        # Summarize and save the episode
                        await save_episode(str(response))
                    print("")
                else:
                    # Default mode
                    response = []
                    tools_in_response = False
                    async for stream_mode, chunk in agent.astream(
                        {"messages": [HumanMessage(content=query)]},
                        stream_mode=["messages", "updates"],
                        config=config
                    ):
                        if stream_mode == "messages":
                            if isinstance(chunk[0], ToolMessage):
                                print(colored("The tool " + chunk[0].name + " was executed.", "yellow"))
                                # For RAG tools or when debug/error info is present, echo tool output
                                try:
                                    tool_output = chunk[0].content or ""
                                    name = getattr(chunk[0], "name", "")
                                    if (
                                        (isinstance(name, str) and name.startswith("rag_"))
                                        or (isinstance(tool_output, str) and ("DEBUG INFO" in tool_output or "ERROR INFO" in tool_output))
                                    ):
                                        print(colored("\n--- Tool Output ---", "cyan"))
                                        print(tool_output)
                                        print(colored("--- End Tool Output ---\n", "cyan"))
                                    else:
                                        print("")
                                except Exception:
                                    print("")
                            elif chunk[1]["langgraph_node"] == "agent":
                                if 'finish_reason' in chunk[0].additional_kwargs:
                                    print(chunk[0].content)
                                    print("")
                                else:
                                    print(chunk[0].content, end="")
                        else:
                            # Construct episode
                            if "pre_model_hook" not in chunk:
                                response.append(chunk)
                            if "tools" in chunk:
                                tools_in_response = True
                    if tools_in_response:
                        # Summarize and save the episode
                        await save_episode(str(response))
            except Exception as e:
                print(colored(f"\nError in user chat: {str(e)}", "red"))
                print(colored(traceback.format_exc(), "red"))

def run_chat(poll_proc, exit_event):
    return asyncio.run(chat(poll_proc, exit_event))

# ==========
# Poll agent
# ==========

# Write Q&A history to exadata_qa file
def write_to_file(
    node_type: str,
    query: str,
    response: str
) -> None:
    node_name = query.split(" ", 1)[0][:-1]
    query = query.split(" ", 1)[1].replace("\"", "").strip()
    with open(EXADATA_QA_PATH, 'a') as file:
        print(f"\n{node_name} ({node_type} node): " + query, file=file)
        print("\nExaCopilot: " + response, file=file)

# Remove quotes from a string since node attribute values cannot have quotes
def remove_quotes(
    text: str
) -> str:
    text = contractions.fix(text)
    text = "".join(text.split('\''))
    text = "".join(text.split('\"'))
    return text

# Poll loop
async def poll(exit_event):
    """
    Poll loop for agent to interact with Exadata.
    """
    async with client:
        tools = await load_mcp_tools(client.session)
        # Ensure every tool has a non-empty description for OCI chat tool schema
        def _ensure_tool_descriptions(ts):
            sanitized = []
            for t in ts:
                try:
                    desc = getattr(t, "description", None)
                    name = getattr(t, "name", t.__class__.__name__)
                    if not desc or not str(desc).strip():
                        setattr(t, "description", f"{name} tool")
                except Exception:
                    pass
                sanitized.append(t)
            return sanitized
        tools = _ensure_tool_descriptions(tools)
        # Create agent
        agent = create_react_agent(
            model=chat_model,
            tools=tools,
            prompt=poll_recurring_prompt
        )
        while True:
            # Poll each node in turn
            for node in DB_NODES:
                if exit_event.is_set():
                    sys.exit(0)
                try:
                    query = get_query(node, "dbserver")
                    response = await agent.ainvoke({"messages": [{"role": "user", "content": query}]})
                    response = response['messages'][-1].content
                    response = remove_quotes(response)
                    set_response(node, "dbserver", response)
                    write_to_file("database", query, response)
                except Exception as e:
                    print(colored(f"\nError in Exadata chat: {str(e)}", "red"))
                    print(colored(traceback.format_exc(), "red"))
            for node in CELL_NODES:
                if exit_event.is_set():
                    sys.exit(0)
                try:
                    query = get_query(node, "cell")
                    response = await agent.ainvoke({"messages": [{"role": "user", "content": query}]})
                    response = response['messages'][-1].content
                    response = remove_quotes(response)
                    set_response(node, "cell", response)
                    write_to_file("cell", query, response)
                except Exception as e:
                    print(colored(f"\nError in Exadata chat: {str(e)}", "red"))
                    print(colored(traceback.format_exc(), "red"))

def run_poll(exit_event):
    return asyncio.run(poll(exit_event))

# ====
# Main
# ====

def main():
    # Apply workarounds
    workarounds.msgpack_patch()
    workarounds.tools_patch()
    workarounds.stream_patch()
    # Execute chat and poll in separate processes
    poll_proc = None
    exit_event = None
    if args.poll:
        # Check that Q&A attributes exist on nodes
        print(colored("Checking that Q&A attributes exist on nodes...\n", "yellow"))
        supported = polling_supported()
        if supported:
            exit_event = multiprocessing.Event()
            poll_proc = multiprocessing.Process(target=run_poll, args=(exit_event,))
            poll_proc.start()
        else:
            print(colored("The fleet does not support the Q&A attributes for polling. ExaCopilot will continue without polling.\n", "yellow"))
    run_chat(poll_proc, exit_event)

if __name__ == "__main__":
    main()
