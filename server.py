from typing import Annotated, Literal
from pydantic import Field
from fastmcp import FastMCP, Context
import configparser
import importlib.util
import sys
from contextlib import redirect_stdout
import io
import shlex
from datetime import datetime
import re

# LangChain imports for RAG
from langchain_community.embeddings import OCIGenAIEmbeddings
from langchain_core.vectorstores import InMemoryVectorStore

# Custom imports
import workarounds

# =====
# Setup
# =====

# Apply workarounds
workarounds.logging_patch()
workarounds.utc_patch()

# Initialize MCP server
mcp = FastMCP("Exadata MCP Server")

# Parse config file
config = configparser.ConfigParser()
config.read('config.ini')
EMBED_MODEL_ID = config.get("OCI", "embed_model_id")
SERVICE_ENDPOINT = config.get("OCI", "service_endpoint")
COMPARTMENT_ID = config.get("OCI", "compartment_id")
DB_NODES = [node.strip() for node in config.get("SYSTEM", "db_nodes").split(',')]
CELL_NODES = [node.strip() for node in config.get("SYSTEM", "cell_nodes").split(',')]
DCLI_PATH = config.get("SYSTEM", "dcli_path")

# Import dcli from path
spec = importlib.util.spec_from_file_location("dcli", DCLI_PATH)
dcli = importlib.util.module_from_spec(spec)
sys.modules["dcli"] = dcli
spec.loader.exec_module(dcli)

def execute_dcli_cmd(cmd: str) -> str:
    """
    Execute a dcli command.

    Args:
        cmd (str): dcli command string of the form "dcli [options] [command]".
    
    Returns:
        str: Output from dcli utility.
    """
    argv = shlex.split(cmd)
    out = io.StringIO()
    with redirect_stdout(out):
        dcli.main(argv)
    if not out.getvalue():
        return "Output is empty."
    return(out.getvalue())

# =========
# RAG setup
# =========

# Initialize embed model
embed_model = OCIGenAIEmbeddings(
    model_id=EMBED_MODEL_ID,
    service_endpoint=SERVICE_ENDPOINT,
    compartment_id=COMPARTMENT_ID,
)

# Load vector stores and initialize retrievers
db_metric_vector_store = InMemoryVectorStore.load(f"rag/db_metric_definitions_{EMBED_MODEL_ID}.pkl", embedding=embed_model)
db_metric_retriever = db_metric_vector_store.as_retriever(search_kwargs={"k": 8})

cell_metric_vector_store = InMemoryVectorStore.load(f"rag/cell_metric_definitions_{EMBED_MODEL_ID}.pkl", embedding=embed_model)
cell_metric_retriever = cell_metric_vector_store.as_retriever(search_kwargs={"k": 8})

dbmcli_help_vector_store = InMemoryVectorStore.load(f"rag/dbmcli_help_{EMBED_MODEL_ID}.pkl", embedding=embed_model)
dbmcli_help_retriever = dbmcli_help_vector_store.as_retriever(search_kwargs={"k": 3})

cellcli_help_vector_store = InMemoryVectorStore.load(f"rag/cellcli_help_{EMBED_MODEL_ID}.pkl", embedding=embed_model)
cellcli_help_retriever = cellcli_help_vector_store.as_retriever(search_kwargs={"k": 3})

dbmcli_describe_vector_store = InMemoryVectorStore.load(f"rag/dbmcli_describe_{EMBED_MODEL_ID}.pkl", embedding=embed_model)
dbmcli_describe_retriever = dbmcli_describe_vector_store.as_retriever(search_kwargs={"k": 3})

cellcli_describe_vector_store = InMemoryVectorStore.load(f"rag/cellcli_describe_{EMBED_MODEL_ID}.pkl", embedding=embed_model)
cellcli_describe_retriever = cellcli_describe_vector_store.as_retriever(search_kwargs={"k": 3})

# ==========
# Poll agent
# ==========

def get_query(
    node: Annotated[
        str,
        Field(description="Single node.")
    ],
    node_type: Annotated[
        Literal["cell", "dbserver"],
        Field(description="Type of node.")
    ]
) -> str:
    """
    Get a query from a node.
    """
    cmd = f"dcli -l root -c {node} 'cellcli -e list {node_type} attributes questionForLlm'"
    return execute_dcli_cmd(cmd)

def set_response(
    node: Annotated[
        str,
        Field(description="Single node.")
    ],
    node_type: Annotated[
        Literal["cell", "dbserver"],
        Field(description="Type of node.")
    ],
    response: Annotated[
        str,
        Field(description="Response from LLM.")
    ]
):
    """
    Set a response on a node.
    """
    cmd = f"dcli -l root -c {node} 'cellcli -e \"alter {node_type} answerFromLlm=\\\"{response}\\\"\"'"
    execute_dcli_cmd(cmd)

def polling_supported() -> bool:
    """
    Check that Q&A attributes exist on every node of the fleet.
    """
    error = "01504: Invalid command syntax."
    db_cmd = f"dcli -l root -c {','.join(DB_NODES)} 'cellcli -e list dbserver attributes questionForLlm,answerFromLlm'"
    if error in execute_dcli_cmd(db_cmd):
        return False
    cell_cmd = f"dcli -l root -c {','.join(CELL_NODES)} 'cellcli -e list cell attributes questionForLlm,answerFromLlm'"
    if error in execute_dcli_cmd(cell_cmd):
        return False
    return True

# =======================
# Generalizable RAG tools
# =======================

@mcp.tool
async def execute_dbmcli_cmd(
    natural_language_request: Annotated[
        str,
        Field(description="Detailed request written in natural language to access or modify the fleet. This request should include a verb.")
    ],
    db_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more database nodes.")
    ],
    ctx: Context
) -> str:
    """
    Execute a command using dbmcli, a command-line interface used to alter or access database nodes.
    This tool can create, describe, drop, and list objects and their attributes as well as perform other administrative tasks.
    This tool is only applicable to database nodes and not applicable to cell nodes.
    """
    db_nodes = "".join(db_nodes.split())
    # Get candidate help documents with RAG
    docs = dbmcli_help_retriever.invoke(natural_language_request)
    doc_dict = {}
    for doc in docs:
        action = doc.metadata["command"]
        help_doc = f"\n\nHelp for {action}:\n" + doc.page_content
        # If action involves an object, get attributes for the object
        obj_attributes = ""
        if len(action.split()) > 1:
            obj = action.split()[1]
            obj_attributes = f"\n\nAttributes for {obj}:\n" + dbmcli_describe_vector_store.similarity_search(query=obj, k=1, filter=lambda doc: doc.metadata["object"] == obj)[0].page_content
        doc_dict[action] = f"Action: {action}" + help_doc + obj_attributes
    docs = "\n\n".join([doc_dict[action] for action in doc_dict.keys()])
    # Sample LLM to get best action
    prompt = f"""
    You will receive a user's natural-language request, along with information for several candidate actions.
    For each action, the information includes the command help document and the attributes for the corresponding object if applicable.
    If none of the actions make sense for the requested task, output "Error: No command exists to perform the request."
    Otherwise, output only the action field of the best action.

    User's natural-language request: {natural_language_request}

    {docs}
    """
    action = await ctx.sample(prompt)
    action = action.text
    if action.startswith("Error:"):
        return action
    doc = doc_dict[action]
    # Sample LLM to construct command
    prompt = f"""
    You will receive a user's natural-language request, along with information about the most relevant dbmcli action: {action}.
    If the user's request includes all information needed to construct the correct dbmcli command, output only a single-line dbmcli command, and nothing else.
    If the command's help documentation lists options in brackets (e.g., [option]), these are optional. If the user's request does not specify any of these options, simply return the base command: {action}.
    If applicable, look at the attributes listed. If any of them are relevant to the user's natural-language request, use them to construct your command.
    If the user's request does not provide enough information, output a statement of what additional information is needed, formatted as "Error: Not enough information. Please revise your query by...."

    User's natural-language request: {natural_language_request}

    {doc}
    """
    llm_output = await ctx.sample(prompt)
    llm_output = llm_output.text
    if llm_output.startswith("Error:"):
        return llm_output
    error = "01504: Invalid command syntax."
    cmd = f"dcli -l root -c {db_nodes} 'dbmcli -e {llm_output}'"
    dcli_output = execute_dcli_cmd(cmd)
    if error in dcli_output:
        return f"""
        Error: Invalid syntax in generated dbmcli command: {llm_output}. Please revise your query.
        """
    return f"Successfully executed dbmcli command: {llm_output}.\n\n{dcli_output}"

@mcp.tool
async def execute_cellcli_cmd(
    natural_language_request: Annotated[
        str,
        Field(description="Detailed request written in natural language to access or modify the fleet. This request should include a verb.")
    ],
    cell_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more cell nodes.")
    ],
    ctx: Context
) -> str:
    """
    Execute a command using CellCLI, a command-line interface used to alter or access cell nodes.
    This tool can create, describe, drop, and list objects and their attributes as well as perform other administrative tasks.
    This tool is only applicable to cell nodes and not applicable to database nodes.
    """
    cell_nodes = "".join(cell_nodes.split())
    # Get candidate help documents with RAG
    docs = cellcli_help_retriever.invoke(natural_language_request)
    doc_dict = {}
    for doc in docs:
        action = doc.metadata["command"]
        help_doc = f"\n\nHelp for {action}:\n" + doc.page_content
        # If action involves an object, get attributes for the object
        obj_attributes = ""
        if len(action.split()) > 1:
            obj = action.split()[1]
            obj_attributes = f"\n\nAttributes for {obj}:\n" + cellcli_describe_vector_store.similarity_search(query=obj, k=1, filter=lambda doc: doc.metadata["object"] == obj)[0].page_content
        doc_dict[action] = f"Action: {action}" + help_doc + obj_attributes
    docs = "\n\n".join([doc_dict[action] for action in doc_dict.keys()])
    # Sample LLM to get best action
    prompt = f"""
    You will receive a user's natural-language request, along with information for several candidate actions.
    For each action, the information includes the command help document and the attributes for the corresponding object if applicable.
    If none of the actions make sense for the requested task, output "Error: No command exists to perform the request."
    Otherwise, output only the action field of the best action.

    User's natural-language request: {natural_language_request}

    {docs}
    """
    action = await ctx.sample(prompt)
    action = action.text
    if action.startswith("Error:"):
        return action
    doc = doc_dict[action]
    # Sample LLM to construct command
    prompt = f"""
    You will receive a user's natural-language request, along with information about the most relevant CellCLI action: {action}.
    If the user's request includes all information needed to construct the correct CellCLI command, output only a single-line CellCLI command, and nothing else.
    If the command's help documentation lists options in brackets (e.g., [option]), these are optional. If the user's request does not specify any of these options, simply return the base command: {action}.
    If applicable, look at the attributes listed. If any of them are relevant to the user's natural-language request, use them to construct your command.
    If the user's request does not provide enough information, output a statement of what additional information is needed, formatted as "Error: Not enough information. Please revise your query by...."

    User's natural-language request: {natural_language_request}

    {doc}
    """
    llm_output = await ctx.sample(prompt)
    llm_output = llm_output.text
    if llm_output.startswith("Error:"):
        return llm_output
    error = "01504: Invalid command syntax."
    cmd = f"dcli -l root -c {cell_nodes} 'cellcli -e {llm_output}'"
    dcli_output = execute_dcli_cmd(cmd)
    if error in dcli_output:
        return f"""
        Error: Invalid syntax in generated CellCLI command: {llm_output}. Please revise your query.
        """
    return f"Successfully executed CellCLI command: {llm_output}.\n\n{dcli_output}"

@mcp.tool
async def get_current_metric(
    description: Annotated[
        str,
        Field(description="Name or detailed description in natural language of metric to get.")
    ],
    cell_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more cell nodes. Use '' if you do not want to call the tool on cell nodes.")
    ],
    db_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more database nodes. Use '' if you do not want to call the tool on database nodes.")
    ],
    ctx: Context
) -> str:
    """
    Get the current value of a specific metric for a set of nodes given the name or description of the metric.
    This tool can be called on just cell nodes, just database nodes, or both cell and database nodes. At least one node must be specified.
    """
    if not cell_nodes and not db_nodes:
        return "Error: At least one node must be specified."
    # Get metric for cell nodes
    if cell_nodes:
        cell_nodes = "".join(cell_nodes.split())
        docs = cell_metric_retriever.invoke(description)
        docs = "\n".join([doc.page_content for doc in docs])
        prompt = f"""
        You will receive a description, along with several candidate metrics.
        If none of the metrics fit the description, output "Error: No cell node metric fits the description."
        Otherwise, output only the name of the best metric.

        Description: {description}
        
        Metrics:
        {docs}
        """
        llm_output = await ctx.sample(prompt)
        llm_output = llm_output.text
        if llm_output.startswith("Error:"):
            cell_result = llm_output
        else:
            cell_cmd = f"dcli -l root -c {cell_nodes} 'cellcli -e list metriccurrent {llm_output} detail'"
            cell_result = execute_dcli_cmd(cell_cmd)
    else:
        cell_result = ""
    # Get metric for database nodes
    if db_nodes:
        db_nodes = "".join(db_nodes.split())
        docs = db_metric_retriever.invoke(description)
        docs = "\n".join([doc.page_content for doc in docs])
        prompt = f"""
        You will receive a description, along with several candidate metrics.
        If none of the metrics fit the description, output "Error: No database node metric fits the description."
        Otherwise, output only the name of the best metric.

        Description: {description}
        
        Metrics:
        {docs}
        """
        llm_output = await ctx.sample(prompt)
        llm_output = llm_output.text
        if llm_output.startswith("Error:"):
            db_result = llm_output
        else:
            db_cmd = f"dcli -l root -c {db_nodes} 'dbmcli -e list metriccurrent {llm_output} detail'"
            db_result = execute_dcli_cmd(db_cmd)
    else:
        db_result = ""
    return cell_result + db_result

# ===================
# Informational tools
# ===================

@mcp.tool
def get_node_info(
    cell_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more cell nodes. Use '' if you do not want to call the tool on cell nodes.")
    ],
    db_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more database nodes. Use '' if you do not want to call the tool on database nodes.")
    ]
) -> str:
    """
    Get general information for a set of nodes.
    This tool can be called on just cell nodes, just database nodes, or both cell and database nodes. At least one node must be specified.
    """
    if not cell_nodes and not db_nodes:
        return "Error: At least one node must be specified."
    # Get cell node info
    if cell_nodes:
        cell_nodes = "".join(cell_nodes.split())
        cell_cmd = f"dcli -l root -c {cell_nodes} 'cellcli -e list cell detail'"
        cell_result = execute_dcli_cmd(cell_cmd)
    else:
        cell_result = ""
    # Get database node info
    if db_nodes:
        db_nodes = "".join(db_nodes.split())
        db_cmd = f"dcli -l root -c {db_nodes} 'cellcli -e list dbserver detail'"
        db_result = execute_dcli_cmd(db_cmd)
    else:
        db_result = ""
    return cell_result + db_result

@mcp.tool
def get_cell_disk_info(
    cell_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more cell nodes.")
    ]
) -> str:
    """
    Get cell disk information for a set of cell nodes.
    This tool is only applicable to cell nodes and not applicable to database nodes.
    """
    cell_nodes = "".join(cell_nodes.split())
    cmd = f"dcli -l root -c {cell_nodes} 'cellcli -e list celldisk detail'"
    return execute_dcli_cmd(cmd)

@mcp.tool
def get_physical_disk_info(
    nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more nodes.")
    ]
) -> str:
    """
    Get physical disk information for a set of nodes.
    """
    nodes = "".join(nodes.split())
    cmd = f"dcli -l root -c {nodes} 'cellcli -e list physicaldisk detail'"
    return execute_dcli_cmd(cmd)

# ===============
# Debugging tools
# ===============

@mcp.tool
def get_alert_history(
    nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more nodes.")
    ]
) -> str:
    """
    Get alert history for a set of nodes.
    Alert history for a given node reveals significant unusual events that occurred on that node.
    """
    nodes = "".join(nodes.split())
    cmd = f"dcli -l root -c {nodes} 'cellcli -e list alerthistory detail'"
    return execute_dcli_cmd(cmd)

@mcp.tool
def get_system_messages(
    nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more nodes.")
    ],
    start_datetime_str: Annotated[
        str,
        Field(description="Start timestamp. It should be formatted as %b %d %H:%M:%S (e.g., 'Jul 16 12:30:58').")
    ],
    end_datetime_str: Annotated[
        str,
        Field(description="End timestamp. It should be formatted as %b %d %H:%M:%S (e.g., 'Jul 16 13:15:21').")
    ]
) -> str:
    """
    Get system messages logged in /var/log/messages on each node in a set of nodes over a given time range.
    System messages reveal actions of processes related to the system of a node. They can help determine causes of events and aid in debugging.
    To check system messages around a certain time, set start_datetime_str to 5 minutes before that time and end_datetime_str to 5 minutes after that time.
    """
    nodes = "".join(nodes.split())
    cmd = f"dcli -l root -c {nodes} cat /var/log/messages"
    data = execute_dcli_cmd(cmd).split("\n")
    # Process start and end datetimes
    current_year_str = str(datetime.now().year)
    datetime_format = "%Y %b %d %H:%M:%S"
    try:
        start_datetime = datetime.strptime(current_year_str + " " + start_datetime_str, datetime_format)
        end_datetime = datetime.strptime(current_year_str + " " + end_datetime_str, datetime_format)
    except ValueError:
        return "Error: Invalid datetime format."
    if start_datetime > end_datetime:
        return "Error: Invalid datetime range."
    # Filter log messages
    filtered_data = ""
    for line in data:
        try:
            timestamp = current_year_str + " " + line.split(" ", 1)[1][:len(start_datetime_str)]
            log_datetime = datetime.strptime(timestamp, datetime_format)
            if start_datetime <= log_datetime <= end_datetime:
                filtered_data += line + "\n"
        except ValueError:
            pass
        except IndexError:
            pass
    if len(filtered_data) > 50000:
        return "Error: The time range is too large. Please specify a shorter time range."
    if not filtered_data:
        return "There are no messages from this time range."
    return filtered_data

@mcp.tool
def get_alert_log(
    nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more nodes.")
    ],
    service_type: Annotated[
        Literal["exascale", "dbserver", "cell"],
        Field(
            description="""
            Type of service to get alert log for. By default, choose "dbserver" for database nodes and "cell" for cell nodes.
            Values:
                "exascale": Exascale services on either cell and/or database nodes.
                "dbserver": Database node services MS and RS on database nodes. This argument is only applicable to database nodes and not applicable to cell nodes.
                "cell": Cell node services CELLSRV, MS, and RS on cell nodes. This argument is only applicable to cell nodes and not applicable to database nodes.
            """
        )
    ],
    start_datetime_str: Annotated[
        str,
        Field(description="Start timestamp. It should be formatted in ISO 8601 format (e.g., '2025-07-16T18:15:07-07:00').")
    ],
    end_datetime_str: Annotated[
        str,
        Field(description="End timestamp. It should be formatted in ISO 8601 format (e.g., '2025-07-21T17:49:01-07:00').")
    ]
) -> str:
    """
    Get alert log messages logged in alert.log and log.xml relating to Exascale services, cell services, or database services on each node in a set of nodes over a given time range.
    Alert log messages reveal actions of processes related to the services on a node. They can help determine causes of events and aid in debugging.
    To check alert log messages around a certain time, set start_datetime_str to 5 minutes before that time and end_datetime_str to 5 minutes after that time.
    """
    if service_type == "exascale":
        log_path = "/var/log/oracle/diag/EXC/exc/`hostname -s`/alert/log.xml"
    else:
        log_path = f"/var/log/oracle/diag/asm/{service_type}/`hostname -s`/alert/log.xml"
    nodes = "".join(nodes.split())
    cmd = f"dcli -l root -c {nodes} cat {log_path}"
    data = execute_dcli_cmd(cmd)
    # Process start and end datetimes
    try:
        start_datetime = datetime.fromisoformat(start_datetime_str)
        end_datetime = datetime.fromisoformat(end_datetime_str)
    except ValueError:
        return "Error: Invalid datetime format."
    if start_datetime > end_datetime:
        return "Error: Invalid datetime range."
    # Filter log messages
    filtered_data = ""
    # Pattern to capture each message block
    msg_pattern = r"([^\n]*?:\s*<msg[^>]+>.*?</msg>)"
    # Pattern to extract the time
    time_pattern = r"time='([\d\-T\:\.]+(?:[\+\-]\d{2}:\d{2})?)'"
    for match in re.finditer(msg_pattern, data, re.DOTALL):
        block = match.group(1)
        time_match = re.search(time_pattern, block)
        if time_match:
            log_datetime = datetime.fromisoformat(time_match.group(1))
            if start_datetime <= log_datetime <= end_datetime:
                filtered_data += block
    if len(filtered_data) > 50000:
        return "Error: The time range is too large. Please specify a shorter time range."
    if not filtered_data:
        return "There are no messages from this time range."
    return filtered_data

@mcp.tool
def hangman(
    cell_node: Annotated[
        str,
        Field(description="Single cell node.")
    ],
    incident_number: Annotated[
        str,
        Field(description="Incident number of hang incident that hangman should examine.")
    ]
) -> str:
    """
    Determine the cause of a CELLSRV hang on a cell node based on an incident number.
    The incident number can be obtained by looking at the associated alert in the alert history and checking the alertAction field.
    This tool can analyze a RS-7445 alert by using hangman to examine the associated incident.
    This tool is only applicable to cell nodes and not applicable to database nodes.
    """
    cmd = f"dcli -l root -c {cell_node} locate -l 1 --regex hangman$"
    hangman_path = execute_dcli_cmd(cmd).split()[1]
    trace_path = f"/opt/oracle/cell/log/diag/asm/cell/`hostname -s`/incident/incdir_{incident_number}/*.trc"
    cmd = f"dcli -l root -c {cell_node} {hangman_path} {trace_path}"
    return execute_dcli_cmd(cmd)

# ====================
# Administrative tools
# ====================

@mcp.tool
def alter_software_update_frequency(
    nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more nodes.")
    ],
    frequency: Annotated[
        Literal["none", "daily", "weekly", "biweekly"],
        Field(description="Frequency of software update. If you do not know what the frequency should be, please prompt the user for it.")
    ] = "none",
) -> str:
    """
    Change software update frequency for a set of nodes.
    """
    nodes = "".join(nodes.split())
    cmd = f"dcli -l root -c {nodes} 'cellcli -e alter softwareupdate frequency={frequency}'"
    return execute_dcli_cmd(cmd)

@mcp.tool
def alter_node_services(
    action: Annotated[
        Literal["shutdown", "restart", "startup"],
        Field(
            description="""
            Action to perform on service.
            Values:
                "shutdown": Shut down the service.
                "restart": Restart the service.
                "startup": Start up the service.
            """
        )
    ],
    service: Annotated[
        Literal["ms", "rs", "cellsrv", "all"],
        Field(
            description="""
            Service to restart.
            Values:
                "ms": Management Server (MS) process.
                "rs": Restart Server (RS) process.
                "cellsrv": CELLSRV process. This argument is only applicable to cell nodes and not applicable to database nodes.
                "all": All services.
            """
        )
    ],
    cell_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more cell nodes. Use '' if you do not want to call the tool on cell nodes.")
    ],
    db_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more database nodes. Use '' if you do not want to call the tool on database nodes.")
    ]
) -> str:
    """
    Shut down, restart, or start up services for a set of nodes.
    This tool can be called on just cell nodes, just database nodes, or both cell and database nodes. At least one node must be specified.
    """
    if not cell_nodes and not db_nodes:
        return "Error: At least one node must be specified."
    # Alter cell node services
    if cell_nodes:
        cell_nodes = "".join(cell_nodes.split())
        cell_cmd = f"dcli -l root -c {cell_nodes} 'cellcli -e alter cell {action} services {service}'"
        cell_result = execute_dcli_cmd(cell_cmd)
    else:
        cell_result = ""
    # Alter database node services
    if db_nodes:
        db_nodes = "".join(db_nodes.split())
        db_cmd = f"dcli -l root -c {db_nodes} 'cellcli -e alter dbserver {action} services {service}'"
        db_result = execute_dcli_cmd(db_cmd)
    else:
        db_result = ""
    return cell_result + db_result
    
@mcp.tool
def examine_alert_history(
    node: Annotated[
        str,
        Field(description="Single node.")
    ],
    id: Annotated[
        str,
        Field(description="Alert ID.")
    ],
    examiner: Annotated[
        str,
        Field(description="Name of examiner.")
    ]
) -> str:
    """
    Mark an alert on a given node as examined by an examiner.
    This tool cannot be used to drop the alert. To drop the alert, execute a CellCLI command.
    """
    cmd = f"dcli -l root -c {node} 'cellcli -e alter alerthistory {id} examinedBy=\"{examiner}\"'"
    return execute_dcli_cmd(cmd)

# ====================
# Low power mode tools
# ====================

@mcp.tool
def alter_low_power_mode_schedule(
    db_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more database nodes.")
    ],
    start_datetime_str: Annotated[
        str,
        Field(description="Start timestamp for low power mode period. It should be formatted in ISO 8601 format (e.g., '2025-07-16T18:15:07-07:00'). If you do not know what the start timestamp should be, please prompt the user for it.")
    ],
    duration: Annotated[
        int,
        Field(
            ge=0,
            le=1440,
            description="Duration of low power mode period in minutes. It should be between 0 and 1440. If you do not know what the duration should be, please prompt the user for it."
        )
    ],
    frequency: Annotated[
        Literal["daily", "weekly"],
        Field(description="Frequency of low power mode period. If you do not know what the frequency should be, please prompt the user for it.")
    ],
    action: Annotated[
        Literal["add", "remove", "overwrite"],
        Field(
            description="""
            Action to perform with the low power mode period on the entire schedule.
            Values:
                "add": Add the low power mode period to the schedule.
                "remove": Remove the low power mode period from the schedule.
                "overwrite": Overwrite the entire schedule with the low power mode period.
            """
        )
    ]
):
    """
    Alter the low power mode schedule on each node in a set of database nodes by adding a low power mode period, removing a low power mode period, or overwriting the entire schedule with a low power mode period.
    A low power mode period is a scheduled period of time during which a node is in low power mode.
    This tool is only applicable to database nodes and not applicable to cell nodes.
    """
    db_nodes = "".join(db_nodes.split())
    try:
        datetime.fromisoformat(start_datetime_str)
    except ValueError:
        return "Error: Invalid timestamp for start of low power mode period."
    if action == "add":
        action = "+"
    elif action == "remove":
        action = "-"
    else:
        action = ""
    cmd = f"dcli -l root -c {db_nodes} 'dbmcli -e \"alter dbserver lowPowerModeSchedule{action}=((startTimestamp=\\\"{start_datetime_str}\\\",durationMinutes={duration},frequency={frequency}))\"'"
    return execute_dcli_cmd(cmd)

@mcp.tool
def clear_low_power_mode_schedule(
    db_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more database nodes.")
    ]
):
    """
    Clear the low power mode schedule on each node in a set of database nodes.
    This tool is only applicable to database nodes and not applicable to cell nodes.
    """
    db_nodes = "".join(db_nodes.split())
    cmd = f"dcli -l root -c {db_nodes} 'dbmcli -e \"alter dbserver lowPowerModeSchedule=null\"'"
    return execute_dcli_cmd(cmd)

@mcp.tool
def alter_low_power_mode(
    db_nodes: Annotated[
        str,
        Field(description="Comma-separated list of one or more database nodes.")
    ],
    status: Annotated[
        Literal["on", "off", "disable"],
        Field(
            description="""
            Status of low power mode.
            Values:
                "on": Turn on low power mode, and keep the low power mode schdeule enabled.
                "off": Turn off low power mode for the current period, but keep the low power mode schedule enabled.
                "disable": Turn off low power mode for the current period, and disable the low power mode schedule.
            """
        )
    ],
    until: Annotated[
        str,
        Field(description="Future end timestamp for the low power mode period. It should be formatted in ISO 8601 format (e.g., '2025-07-16T18:15:07-07:00'). This parameter is only required for turning on low power mode. If you are turning on low power mode and do not know what the end timestamp should be, please prompt the user for the end timestamp.")
    ] = None,
):
    """
    Turn on/off low power mode and enable/disable the low power mode schedule on each node in a set of database nodes.
    This tool is only applicable to database nodes and not applicable to cell nodes.
    """
    db_nodes = "".join(db_nodes.split())
    if status == "on":
        if not until:
            return "Error: Missing timestamp for end of low power mode period."
        try:
            datetime.fromisoformat(until)
        except ValueError:
            return "Error: Invalid timestamp for end of low power mode period."
        value = f"\\\"{until}\\\""
    elif status == "disable":
        value = "never"
    else:
        value = "\\\"\\\""
    cmd = f"dcli -l root -c {db_nodes} 'dbmcli -e \"alter dbserver lowPowerModeUntil={value}\"'"
    return execute_dcli_cmd(cmd)

# ====
# Main
# ====

if __name__ == "__main__":
    mcp.run(transport='stdio')