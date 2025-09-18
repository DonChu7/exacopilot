from typing import Annotated, Any
from pydantic import Field
from fastmcp import FastMCP, Context
import configparser
import importlib
import sys
import os
import json
from contextlib import contextmanager
import traceback

import workarounds
workarounds.logging_patch()
workarounds.utc_patch()

mcp = FastMCP("RAG MCP Server")

# Read RAG agent settings from config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# Support either [RAG] or [rag] section names and strip any trailing spaces.
_rag_section = "RAG" if config.has_section("RAG") else ("rag" if config.has_section("rag") else None)
def _get_rag(key: str, default: str = "") -> str:
    if _rag_section is None:
        return default
    return config.get(_rag_section, key, fallback=default).strip()

RAG_AGENT_PATH = _get_rag("agent_repo", "")
RAG_ENTRY_MODULE = _get_rag("entry_module", "")
RAG_WORKDIR = _get_rag("workdir", "")
_FORCE_DEBUG_VAL = _get_rag("force_debug", "false").lower()
FORCE_DEBUG = _FORCE_DEBUG_VAL in ("1", "true", "yes", "on")
STORE_PATH = _get_rag("store_path", "chroma_db")
COLLECTION = _get_rag("collection", "Repository Collection")
_SKIP_ANALYSIS_VAL = _get_rag("skip_analysis", "false").lower()
SKIP_ANALYSIS = _SKIP_ANALYSIS_VAL in ("1", "true", "yes", "on")
ENV_FILE = _get_rag("env_file", "")
_LOAD_ENV_VAL = _get_rag("load_env", "true").lower()
LOAD_ENV = _LOAD_ENV_VAL in ("1", "true", "yes", "on")
FACTORY_LLM_FUNC = _get_rag("factory_llm_func", "make_llm")
_FACTORY_LLM_KWARGS_RAW = _get_rag("factory_llm_kwargs", "")
_VECTOR_KWARGS_RAW = _get_rag("vector_kwargs", "")
try:
    FACTORY_LLM_KWARGS = json.loads(_FACTORY_LLM_KWARGS_RAW) if _FACTORY_LLM_KWARGS_RAW else {}
except json.JSONDecodeError:
    FACTORY_LLM_KWARGS = {}
try:
    VECTOR_KWARGS = json.loads(_VECTOR_KWARGS_RAW) if _VECTOR_KWARGS_RAW else {}
except json.JSONDecodeError:
    # Fallback simple key=val;key2=val2 parser
    VECTOR_KWARGS = {}
    for part in _VECTOR_KWARGS_RAW.split(";"):
        if not part.strip():
            continue
        if "=" in part:
            k, v = part.split("=", 1)
            VECTOR_KWARGS[k.strip()] = v.strip()

if not RAG_AGENT_PATH or not RAG_ENTRY_MODULE:
    print("Error: [RAG] agent_repo and entry_module must be set in config.ini.")
    # We still start the server so the client can report the error in tool calls

# Try import of the agent module dynamically
rag_module = None
if RAG_AGENT_PATH and RAG_ENTRY_MODULE:
    if RAG_AGENT_PATH not in sys.path:
        sys.path.insert(0, RAG_AGENT_PATH)
    try:
        rag_module = importlib.import_module(RAG_ENTRY_MODULE)
    except Exception as e:
        print(f"Error importing RAG module '{RAG_ENTRY_MODULE}' from '{RAG_AGENT_PATH}': {e}")
        pass

# Track build errors for clearer diagnostics
BUILD_ERRORS = {"llm": None, "vector": None}
VECTOR_BUILD_ROUTE = None

def _format_result(result: Any) -> str:
    """Best-effort normalization of typical RAG results to a string."""
    try:
        # Document-like objects
        if isinstance(result, list):
            # Extract content if it looks like LangChain Documents
            if result and hasattr(result[0], "page_content"):
                return "\n\n".join(
                    f"- {getattr(doc, 'page_content', str(doc))}" for doc in result
                )
            # List of dicts or strings
            return json.dumps(result, ensure_ascii=False, indent=2)
        if isinstance(result, dict):
            # Common patterns: {"answer": ..., "sources": ...}
            if "answer" in result and "sources" in result:
                try:
                    sources = result["sources"]
                    if isinstance(sources, list):
                        src_txt = "\n".join(str(s) for s in sources)
                    else:
                        src_txt = str(sources)
                    return f"{result['answer']}\n\nSources:\n{src_txt}"
                except Exception:
                    pass
            return json.dumps(result, ensure_ascii=False, indent=2)
        return str(result)
    except Exception:
        return str(result)

def _call_with_fallback(fn, query: str, top_k: int | None):
    """Try a few common function signatures for agent search fns."""
    # Try a mix of positional and keyword variations used by RAG agents
    for attempt in (
        lambda: fn(query=query, k=top_k),
        lambda: fn(query=query, top_k=top_k),
        lambda: fn(q=query, k=top_k),
        lambda: fn(text=query, k=top_k),
        lambda: fn(query, top_k),
        lambda: fn(query, k=top_k),
        lambda: fn(query),
        lambda: fn(text=query),
    ):
        try:
            if top_k is None and attempt.__code__.co_argcount >= 2:
                # Skip attempts that will force a k/top_k when not provided
                continue
            return attempt()
        except TypeError:
            continue
    # Last resort
    return fn(query)

@contextmanager
def _in_rag_cwd():
    """Temporarily chdir into the RAG agent repo or explicit workdir."""
    prev = os.getcwd()
    target = RAG_WORKDIR or RAG_AGENT_PATH
    try:
        if target and os.path.isdir(target):
            os.chdir(target)
        # Load .env if present/configured
        try:
            from dotenv import load_dotenv as _ld
            ENV_FILE = config.get(_rag_section, 'env_file', fallback='').strip() if _rag_section else ''
            LOAD_ENV = config.get(_rag_section, 'load_env', fallback='true').strip().lower() in ('1','true','yes','on') if _rag_section else True
            if LOAD_ENV:
                if ENV_FILE and os.path.isfile(ENV_FILE):
                    _ld(ENV_FILE, override=False)
                elif os.path.isfile(os.path.join(os.getcwd(), '.env')):
                    _ld(os.path.join(os.getcwd(), '.env'), override=False)
        except Exception:
            pass
        yield
    finally:
        try:
            os.chdir(prev)
        except Exception:
            pass

def _call_agent_object(obj: Any, query: str, top_k: int | None):
    # No longer used; retained for backward compatibility if needed in future
    raise NotImplementedError

def _build_llm():
    llm_builder = getattr(rag_module, FACTORY_LLM_FUNC, None) if rag_module else None
    if callable(llm_builder):
        try:
            return llm_builder(**FACTORY_LLM_KWARGS) if FACTORY_LLM_KWARGS else llm_builder()
        except Exception as e:
            # record error for diagnostics
            try:
                BUILD_ERRORS
            except NameError:
                pass
            else:
                BUILD_ERRORS["llm"] = repr(e)
            return None
    return None

def _resolve_vector_class():
    """Resolve a VectorStore class with preference for Oracle store when requested.

    Resolution order:
    1) [RAG].vector_class if provided (supports "Class" or "module.Class")
    2) rag_module.OraDBVectorStore if present
    3) importlib.import_module("OraDBVectorStore").OraDBVectorStore
    4) rag_module.VectorStore
    """
    prefer = _get_rag("vector_class", "")
    if prefer:
        try:
            if "." in prefer:
                mod_name, cls_name = prefer.rsplit(".", 1)
                mod = importlib.import_module(mod_name)
                return getattr(mod, cls_name)
            # try on rag_module first
            cls = getattr(rag_module, prefer, None) if rag_module else None
            if cls is not None:
                return cls
            # then as top-level module
            mod = importlib.import_module(prefer)
            # if module was given instead of class, try same-name class
            return getattr(mod, prefer, None)
        except Exception:
            pass
    # Try common Oracle store name on current module
    cls = getattr(rag_module, "OraDBVectorStore", None) if rag_module else None
    if cls is not None:
        return cls
    # Try importing sibling module OraDBVectorStore in the agent repo
    try:
        mod = importlib.import_module("OraDBVectorStore")
        return getattr(mod, "OraDBVectorStore", None)
    except Exception:
        pass
    # Fallback to generic VectorStore from agent
    return getattr(rag_module, "VectorStore", None) if rag_module else None


def _build_vector_store():
    VS = _resolve_vector_class()
    if VS is None:
        return None
    # Preference order:
    # 1) Explicit kwargs from config (e.g., Oracle vector DB params)
    # 2) No-arg constructor (env-driven)
    # 3) Local on-disk store via persist_directory
    global VECTOR_BUILD_ROUTE
    try:
        if VECTOR_KWARGS:
            VECTOR_BUILD_ROUTE = "kwargs"
            return VS(**VECTOR_KWARGS)
    except Exception as e:
        BUILD_ERRORS["vector"] = repr(e)
        return None
    try:
        VECTOR_BUILD_ROUTE = "no-arg"
        return VS()
    except TypeError:
        pass
    try:
        if 'persist_directory' in getattr(VS.__init__, '__code__', type('c',(),{'co_varnames':()})).co_varnames:
            VECTOR_BUILD_ROUTE = "persist_directory"
            return VS(persist_directory=STORE_PATH)
        # As a last resort, try passing the path positionally
        VECTOR_BUILD_ROUTE = "positional_path"
        return VS(STORE_PATH)
    except Exception as e:
        BUILD_ERRORS["vector"] = repr(e)
        return None

def _build_rag_agent(use_cot: bool):
    AgentClass = getattr(rag_module, "RAGAgent", None) if rag_module else None
    if AgentClass is None:
        return None
    llm = _build_llm()
    vs = _build_vector_store()
    if llm is None or vs is None:
        return None
    try:
        return AgentClass(vs, llm, use_cot=use_cot, collection=COLLECTION, skip_analysis=SKIP_ANALYSIS)
    except TypeError:
        try:
            return AgentClass(vs, llm, use_cot=use_cot, collection=COLLECTION)
        except Exception:
            return None

def _run_agent(query: str, use_cot: bool, top_k: int, debug: bool) -> str:
    with _in_rag_cwd():
        agent = _build_rag_agent(use_cot=use_cot)
        if agent is None:
            # include build errors if captured
            errors = []
            try:
                llm_err = BUILD_ERRORS.get("llm")
                vec_err = BUILD_ERRORS.get("vector")
                if llm_err:
                    errors.append(f"LLM error: {llm_err}")
                if vec_err:
                    errors.append(f"VectorStore error: {vec_err}")
            except Exception:
                pass
            detail = " | ".join(errors) if errors else "LLM or VectorStore not available"
            return f"Error: Failed to build RAGAgent ({detail})."
        try:
            result = agent.process_query(query)
        except Exception as e:
            return _build_error_report(f"class:RAGAgent.process_query:use_cot={use_cot}", e) if (debug or FORCE_DEBUG) else f"Error: {e}"
    out = _format_result(result)
    if debug or FORCE_DEBUG:
        dbg = _build_debug_report(result, f"class:RAGAgent.process_query:use_cot={use_cot}")
        return f"{out}\n\n{dbg}"
    return out

def _extract_docs(result: Any) -> list:
    """Attempt to extract a list of document-like objects from result."""
    docs = []
    try:
        if isinstance(result, list):
            docs = result
        elif isinstance(result, dict):
            # Common keys that may hold retrieved docs
            for key in ("docs", "documents", "retrieved", "sources", "results"):
                val = result.get(key)
                if isinstance(val, list):
                    docs = val
                    break
    except Exception:
        pass
    return docs

def _build_debug_report(result: Any, fn_name: str) -> str:
    """Compose a human-readable debug summary: counts, indices, and a simple table."""
    cwd = os.getcwd()
    workdir = RAG_WORKDIR or RAG_AGENT_PATH
    module_file = getattr(rag_module, "__file__", "<unknown>") if rag_module else "<not loaded>"
    docs = _extract_docs(result)
    doc_count = len(docs) if isinstance(docs, list) else 0
    # Collect index/namespace hints
    indices = set()
    rows = []
    for i, d in enumerate(docs[:50]):
        meta = None
        content = None
        score = None
        index_name = None
        try:
            # LangChain Document
            if hasattr(d, "metadata"):
                meta = getattr(d, "metadata", {})
                content = getattr(d, "page_content", None)
            elif isinstance(d, dict):
                meta = d.get("metadata") if isinstance(d.get("metadata"), dict) else {}
                content = d.get("page_content") or d.get("content")
            # Extract score/index
            if meta:
                score = meta.get("score") or meta.get("similarity") or meta.get("distance")
                index_name = (
                    meta.get("index")
                    or meta.get("index_name")
                    or meta.get("namespace")
                    or meta.get("collection")
                )
                if index_name:
                    indices.add(str(index_name))
            source = None
            if meta:
                source = meta.get("source") or meta.get("id") or meta.get("document_id")
            rows.append({
                "#": i + 1,
                "source": str(source) if source is not None else "",
                "score": score if score is not None else "",
                "index": str(index_name) if index_name is not None else "",
            })
        except Exception:
            continue

    # Build a simple text table
    header = f"{'#':<3} {'source':<50} {'score':<10} {'index':<30}"
    sep = "-" * len(header)
    body_lines = []
    for r in rows:
        body_lines.append(
            f"{r['#']:<3} {r['source'][:50]:<50} {str(r['score'])[:10]:<10} {r['index'][:30]:<30}"
        )
    indices_str = ", ".join(sorted(indices)) if indices else ""
    table = "\n".join([header, sep] + body_lines) if rows else "(no documents to display)"
    return (
        "DEBUG INFO\n"
        f"- function: {fn_name}\n"
        f"- module: {module_file}\n"
        f"- cwd: {cwd}\n"
        f"- workdir: {workdir}\n"
        f"- doc_count: {doc_count}\n"
        f"- indices: {indices_str}\n\n"
        f"{table}"
    )

def _build_error_report(fn_name: str, e: Exception) -> str:
    cwd = os.getcwd()
    workdir = RAG_WORKDIR or RAG_AGENT_PATH
    module_file = getattr(rag_module, "__file__", "<unknown>") if rag_module else "<not loaded>"
    tb = traceback.format_exc()
    return (
        "ERROR INFO\n"
        f"- function: {fn_name}\n"
        f"- module: {module_file}\n"
        f"- cwd: {cwd}\n"
        f"- workdir: {workdir}\n"
        f"- sys.path[0]: {sys.path[0] if sys.path else ''}\n"
        f"- exception: {repr(e)}\n\n"
        f"Traceback:\n{tb}"
    )

## legacy: removed old resolver-based implementation

@mcp.tool
async def rag_standard_search(
    query: Annotated[
        str,
        Field(description="Search query for standardized RAG search."),
    ],
    top_k: Annotated[
        int,
        Field(ge=1, le=50, description="Results to retrieve."),
    ] = 5,
    debug: Annotated[
        bool,
        Field(description="If true, include retrieved table, counts, and index info."),
    ] = False,
    ctx: Context = None,
) -> str:
    """Standard RAG retrieval without chain-of-thought.

    Use this for quick fact lookup, short answers, citations, or when you only
    need to pull relevant context from the Repository/PDF/Web collections and
    synthesize a concise response. Prefer this for:
    - definition/lookup questions
    - single-operation or single-file references
    - when latency should be minimized
    """
    return _run_agent(query=query, use_cot=False, top_k=top_k, debug=debug)

async def _rag_cot_search_impl(query: str, top_k: int = 5, debug: bool = False) -> str:
    return _run_agent(query=query, use_cot=True, top_k=top_k, debug=debug)

@mcp.tool
async def rag_cot_search(
    query: Annotated[
        str,
        Field(description="Search query for chain-of-thought RAG search."),
    ],
    top_k: Annotated[
        int,
        Field(ge=1, le=50, description="Results to retrieve."),
    ] = 5,
    debug: Annotated[
        bool,
        Field(description="If true, include retrieved table, counts, and index info."),
    ] = False,
    ctx: Context = None,
) -> str:
    """Chain-of-thought RAG for procedures, workflows, and troubleshooting.

    Choose this when the user asks for step-by-step how-tos, comprehensive
    guides, multi-operation workflows, root-cause analysis or troubleshooting.
    It runs the full planner → researcher → reasoner → synthesizer pipeline and
    will be slower but produces structured, actionable output.
    """
    return await _rag_cot_search_impl(query=query, top_k=top_k, debug=debug)

## debug variants removed to reduce surface area; use `debug=true` arg if needed

@mcp.tool
def rag_debug_info() -> str:
    """Return diagnostic info about the RAG server environment and module loading."""
    cwd = os.getcwd()
    workdir = RAG_WORKDIR or RAG_AGENT_PATH
    module_file = getattr(rag_module, "__file__", "<unknown>") if rag_module else "<not loaded>"
    funcs = []
    if rag_module:
        try:
            funcs = [
                name for name in dir(rag_module)
                if not name.startswith("_") and callable(getattr(rag_module, name, None))
            ][:100]
        except Exception:
            pass
    return (
        f"cwd: {cwd}\n"
        f"workdir: {workdir}\n"
        f"agent_repo: {RAG_AGENT_PATH}\n"
        f"entry_module: {RAG_ENTRY_MODULE}\n"
        f"module_file: {module_file}\n"
        f"store_path: {STORE_PATH}\n"
        f"vector_kwargs: {json.dumps(VECTOR_KWARGS)}\n"
        f"collection: {COLLECTION}\n"
        f"skip_analysis: {SKIP_ANALYSIS}\n"
        f"env_file: {ENV_FILE}\n"
        f"load_env: {LOAD_ENV}\n"
        f"build_error_llm: {BUILD_ERRORS.get('llm')}\n"
        f"build_error_vector: {BUILD_ERRORS.get('vector')}\n"
        f"vector_build_route: {VECTOR_BUILD_ROUTE}\n"
        f"factory_llm_func: {FACTORY_LLM_FUNC}\n"
        f"factory_llm_kwargs: {json.dumps(FACTORY_LLM_KWARGS)}\n"
        f"sys.path[0]: {sys.path[0] if sys.path else ''}\n"
        f"available_callables: {', '.join(funcs)}\n"
    )

## probe tool removed; rely on `debug=true` responses for diagnostics

## introspection tool removed to reduce surface area

if __name__ == "__main__":
    mcp.run(transport="stdio")
