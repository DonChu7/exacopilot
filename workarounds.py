# =========================
# Message serialization bug
# =========================

# https://github.com/langchain-ai/langchain-community/issues/182

from typing import Any, Dict
from langchain_community.chat_models.oci_generative_ai import CohereProvider, _format_oci_tool_calls

def make_json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: make_json_safe(v) for k, v in value.items()}
    elif isinstance(value, list):
        return [make_json_safe(v) for v in value]
    elif hasattr(value, "__dict__"):
        return make_json_safe(vars(value))
    elif hasattr(value, "_asdict"):
        return make_json_safe(value._asdict())
    elif isinstance(value, (str, int, float, bool)) or value is None:
        return value
    else:
        return str(value)

def chat_generation_info(self, response: Any) -> Dict[str, Any]:
    chat_response = response.data.chat_response

    generation_info: Dict[str, Any] = {
        "documents": make_json_safe(chat_response.documents),
        "citations": make_json_safe(chat_response.citations),
        "search_queries": make_json_safe(chat_response.search_queries),
        "is_search_required": chat_response.is_search_required,
        "finish_reason": chat_response.finish_reason,
    }

    if getattr(chat_response, "tool_calls", None):
        generation_info["tool_calls"] = make_json_safe(
            _format_oci_tool_calls(chat_response.tool_calls)
        )

    return generation_info

def msgpack_patch():
    setattr(CohereProvider, "chat_generation_info", chat_generation_info)

# ===========================
# Stripping of tool calls bug
# ===========================

from typing import Sequence, Union, List
from langchain_core.messages import (
    AIMessage,
    ChatMessage,
    HumanMessage,
    ToolMessage,
)

def messages_to_oci_params(
    self, messages: Sequence[ChatMessage], **kwargs: Any
) -> Dict[str, Any]:
    is_force_single_step = kwargs.get("is_force_single_step") or False

    oci_chat_history = []

    for msg in messages[:-1]:
        if self.get_role(msg) == "USER" or self.get_role(msg) == "SYSTEM":
            oci_chat_history.append(
                self.oci_chat_message[self.get_role(msg)](message=msg.content)
            )
        elif isinstance(msg, AIMessage):
            if msg.tool_calls and is_force_single_step:
                continue
            tool_calls = (
                [
                    self.oci_tool_call(name=tc["name"], parameters=tc["args"])
                    for tc in msg.tool_calls
                ]
                if msg.tool_calls
                else None
            )
            msg_content = msg.content if msg.content else " "
            oci_chat_history.append(
                self.oci_chat_message[self.get_role(msg)](
                    message=msg_content, tool_calls=tool_calls
                )
            )
        
        # START OF NEW CODE
        elif isinstance(msg, ToolMessage):
            oci_chat_history.append(
                self.oci_chat_message[self.get_role(msg)](
                    tool_results=[
                        self.oci_tool_result(
                            call=self.oci_tool_call(
                                name=msg.name, parameters={}
                            ),
                            outputs=[{"output": msg.content}],
                        )
                    ],
                )
            )
        # END OF NEW CODE

    # Get the messages for the current chat turn
    current_chat_turn_messages = []
    # START OF OLD CODE
    # for message in messages[::-1]:
    #     current_chat_turn_messages.append(message)
    #     if isinstance(message, HumanMessage):
    #         break
    # END OF OLD CODE
    # START OF NEW CODE
    for i, message in enumerate(messages[::-1]):
        current_chat_turn_messages.append(message)
        if isinstance(message, HumanMessage):
            if len(messages) > i and isinstance(messages[len(messages) - i - 2], ToolMessage):
                # add dummy message REPEATING the tool_result to avoid the error about ToolMessage needing to be followed by an AI message
                oci_chat_history.append(self.oci_chat_message['CHATBOT'](message=messages[len(messages) - i - 2].content))
            break
    # END OF NEW CODE
    current_chat_turn_messages = current_chat_turn_messages[::-1]

    oci_tool_results: Union[List[Any], None] = []
    for message in current_chat_turn_messages:
        if isinstance(message, ToolMessage):
            tool_message = message
            previous_ai_msgs = [
                message
                for message in current_chat_turn_messages
                if isinstance(message, AIMessage) and message.tool_calls
            ]
            if previous_ai_msgs:
                previous_ai_msg = previous_ai_msgs[-1]
                for lc_tool_call in previous_ai_msg.tool_calls:
                    if lc_tool_call["id"] == tool_message.tool_call_id:
                        tool_result = self.oci_tool_result()
                        tool_result.call = self.oci_tool_call(
                            name=lc_tool_call["name"],
                            parameters=lc_tool_call["args"],
                        )
                        tool_result.outputs = [{"output": tool_message.content}]
                        oci_tool_results.append(tool_result)

    if not oci_tool_results:
        oci_tool_results = None

    message_str = "" if oci_tool_results else messages[-1].content

    oci_params = {
        "message": message_str,
        "chat_history": oci_chat_history,
        "tool_results": oci_tool_results,
        "api_format": self.chat_api_format,
    }

    return {k: v for k, v in oci_params.items() if v is not None}

def tools_patch():
    setattr(CohereProvider, "messages_to_oci_params", messages_to_oci_params)

# ==================
# Extraneous logging
# ==================

from fastmcp.server.server import FastMCP
from mcp.server.stdio import stdio_server
from mcp.server.lowlevel.server import NotificationOptions

async def run_stdio_async(self, show_banner: bool = True) -> None:
    """Run the server using stdio transport."""
    async with stdio_server() as (read_stream, write_stream):
        await self._mcp_server.run(
            read_stream,
            write_stream,
            self._mcp_server.create_initialization_options(
                NotificationOptions(tools_changed=True)
            ),
        )

def logging_patch():
    setattr(FastMCP, "run_stdio_async", run_stdio_async)

# ==========================
# Deprecated datetime object
# ==========================

from oci import base_client
from datetime import datetime
from zoneinfo import ZoneInfo

def utc_now():
    return " " + str(datetime.now(ZoneInfo('UTC'))) + ": "

def utc_patch():
    base_client.utc_now = utc_now

# =================
# JSON decode error
# =================

from langchain_community.chat_models.oci_generative_ai import ChatOCIGenAI
from typing import Optional, Iterator
from langchain_core.messages import BaseMessage, AIMessageChunk
from langchain_core.messages.tool import ToolCallChunk
from langchain_core.callbacks import CallbackManagerForLLMRun
from langchain_core.outputs import ChatGenerationChunk
import json

def _stream(
    self,
    messages: List[BaseMessage],
    stop: Optional[List[str]] = None,
    run_manager: Optional[CallbackManagerForLLMRun] = None,
    **kwargs: Any,
) -> Iterator[ChatGenerationChunk]:
    request = self._prepare_request(messages, stop=stop, stream=True, **kwargs)
    response = self.client.chat(request)

    for event in response.data.events():
        try:
            event_data = json.loads(event.data)
        except json.JSONDecodeError as e:
            print("Failed to parse event.data:", repr(event.data))
            print(e)
        if not self._provider.is_chat_stream_end(event_data):  # still streaming
            delta = self._provider.chat_stream_to_text(event_data)
            chunk = ChatGenerationChunk(message=AIMessageChunk(content=delta))
            if run_manager:
                run_manager.on_llm_new_token(delta, chunk=chunk)
            yield chunk
        else:  # stream end
            generation_info = self._provider.chat_stream_generation_info(event_data)
            tool_call_chunks = []
            if tool_calls := generation_info.get("tool_calls"):
                content = self._provider.chat_stream_to_text(event_data)
                try:
                    tool_call_chunks = [
                        ToolCallChunk(
                            name=tool_call["function"].get("name"),
                            args=tool_call["function"].get("arguments"),
                            id=tool_call.get("id"),
                            index=tool_call.get("index"),
                        )
                        for tool_call in tool_calls
                    ]
                except KeyError:
                    pass
            else:
                content = ""
            message = AIMessageChunk(
                content=content,
                additional_kwargs=generation_info,
                tool_call_chunks=tool_call_chunks,
            )
            yield ChatGenerationChunk(
                message=message,
                generation_info=generation_info,
            )

def stream_patch():
    setattr(ChatOCIGenAI, "_stream", _stream)