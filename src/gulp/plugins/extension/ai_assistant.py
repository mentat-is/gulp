"""
Plugin: Threat hunting AI-Assistant

This Plugin implements an extension for Gulp that delegates threat-hunting
analysis and correlation of log events to an external LLM (via the OpenRouter
chat completions API). It exposes a single API endpoint that accepts up to 10
log event dictionaries and streams back an AI-generated analysis to clients
through an internal Redis broker.

Configuration:
Requires the following configuration in the GULP configuration file:
"ai_report": {
    // The default model to use.
    "default_model": "your_default_model_here",

    // Insert your OpenRouter API key here.
    "openrouter_key": "sk-or-your-openrouter-api-key",
}

Behavior:
- Registers POST /get_ai_hint during post_init when running in the main process.
- Reads configuration from GulpConfig under the 'ai_report' section:
    - 'openrouter_key' (required): API key used in the Authorization header.
    - 'default_model' (required): model identifier to send in the API payload.
- Validates read permission for the requested operation before accepting work.
- Constructs a prompt by injecting the provided event list into a fixed prompt
    template and spawns a background task to call the AI service.
- Returns a JSend pending response immediately while processing happens
    asynchronously in the background.

Exposed API:
The plugin exposes a single HTTP endpoint:

- Endpoint: `/get_ai_hint`
- Method: `POST`
- Response Behavior: The API operates asynchronously. Upon receiving a request, it performs permission checks and immediately returns a JSON response with a `pending` status.
- Events written to the Redis broker (GulpRedisBroker) include:
    - "ai_assistant_stream": partial streamed content (d field contains text).
    - "ai_assistant_done": indicates the stream is finished.
    - "ai_assistant_error": indicates an API error (d field contains error text).
- Clients are expected to subscribe to these events to reconstruct the final
        AI-generated report in real time.

Input Data Requirements
To use the API, the client must provide the following parameters via query string:
    - token (mandatory),
    - operation_id (mandatory),
    - ws_id (mandatory)
    - req_if (optional)

and send data via body:
    - data payload must be a list of JSON objects (maximum 10 items) representing the logs to be analyzed.
    Each object in the list requires the following fields:
        - event.original: The raw, unstructured text of the log (e.g., the full log line from IIS or a firewall).
        - timestamp: The time the event occurred, used by the AI to establish a timeline.
        - agent.type: The source or tool that generated the log (e.g., "iis", "windows-eventlog", "firewall").
        - gulp.source_id: The unique ID for the log source (e.g., a specific log file).
        - gulp.context_id: "The unique ID for the context (e.g., a hostname or server).",
"""

import asyncio
import aiohttp
from fastapi import Body, Depends, Query
import json
from typing import Annotated, override

from fastapi.responses import JSONResponse
import orjson
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import GulpRequestStats, RequestCanceledError
from gulp.api.collab.structs import GulpUserPermission
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from gulp.api.server_api import GulpServer
from gulp.api.ws_api import GulpRedisBroker
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase, GulpPluginType

from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger


class Plugin(GulpPluginBase):

    _api_key: str = None
    _model: str = None

    _PROMPT = """# Role and Goal
    You are a security analyst. Your task is to analyze a set of log events provided in a JSON array. The logs come from different systems (like firewalls, web servers, and Windows endpoints).

    **This is the most important instruction:**
    The JSON data provides a field called `event.original`. This field contains the raw, unstructured log text. **You MUST parse this `event.original` text** to find entities like IP addresses, hostnames, usernames, file hashes, or process names.

    ## Your Goal: You must perform three tasks
    1. **Analyze Events:** Briefly explain what each individual event means.
    2. **Assess Correlation:** Determine if the events are **CORRELATED** or **NOT CORRELATED**.
    3. **Explain Findings:**
    - If **CORRELATED**, explain *how* they are linked (which entities do they share?) and describe the sequence of the incident.
    - If **NOT CORRELATED**, state this clearly.

    ## Correlation Rules
    Events are **CORRELATED** if they meet both conditions:
    1.  They share at least one common entity (indicator) that you **find inside the `event.original` text** (such as a shared IP address, hostname, username, file hash, or process name). Entities must be exact matches (use case-insensitive).

    Assume events are provided in the order they appear in the JSON array, but sort them chronologically by the `timestamp` field when describing sequences. If events lack required fields (e.g., `agent.type`), note this in your analysis and proceed with available data. For multiple events, correlation applies to the entire set: all events must share at least one entity with at least one other event in the set, forming a connected chain.

    ## Input Format Example
    The input is a JSON array. **This is a description about any fields inside JSON object.
    ```json
    [
    {
        "@timestamp": "The ISO 8601 timestamp for when the event occurred.",
        "gulp.source_id": "The unique ID for the log source (e.g., a specific log file).",
        "gulp.context_id": "The unique ID for the context (e.g., a hostname or server).",
        "agent.type": "The tool or plugin that generated the log (e.g., 'sysmon', 'firewall').",
        "event.original": "The orginal log write by agent.type"
    }
    ]
    ```
    Input Data:
    <%DATA%>

    ## Required Output Format
    You must provide your analysis in Markdown using this exact structure:
    1. **Event Analysis**
    - Event 1 (agent.type: [agent_type]): [brief event description]
    - Event 2 (agent.type: [agent_type]): [brief event description]
    - ... (Continue for all events)
    2. **Correlation Assessment**  
    (State here: CORRELATED or NOT CORRELATED)
    3. **Incident Summary**  
    (If NOT CORRELATED, write: "The events do not share common indicators and are considered unrelated.")  
    (If CORRELATED, explain the incident sequence here.)"""

    @override
    def desc(self) -> str:
        return "ai assistant, help user to understand events and correlation from different logs"

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self) -> str:
        return "Threat hunting AI-Assistant"

    @override
    async def post_init(self, *kwargs):
        if self.is_running_in_main_process():
            GulpServer.get_instance().add_api_route(
                "/get_ai_hint",
                self.get_ai_hint_handler,
                methods=["POST"],
                response_model=JSendResponse,
                response_model_exclude_none=True,
                tags=["analysis"],
                responses={
                    200: {
                        "content": {
                            "application/json": {
                                "example": {
                                    "status": "success",
                                    "data": "the incident report in markdown format",
                                }
                            }
                        }
                    }
                },
                summary="tell to explain and correlate logs event",
                description="tell to ai to explain the events logs provided if there are some correlation about this events based by context and timeline",
            )
            # read api key
            self._api_key = None
            ai_report = GulpConfig.get_instance().get("ai_report", {})

            if ai_report:
                self._api_key = ai_report.get("openrouter_key", None)
                self._model = ai_report.get("default_model")
            if not self._api_key:
                raise Exception(
                    "'ai_report/openrouter_key' missing in the configuration file!"
                )
            if not self._model:
                raise Exception(
                    "'ai_report/default_model' missing in the configuration file!"
                )
        else:
            MutyLogger.get_instance().debug("initialized in worker process")

    async def get_ai_hint_handler(
        self,
        token: Annotated[str, Depends(APIDependencies.param_token)],
        operation_id: Annotated[str, Depends(APIDependencies.param_operation_id)],
        ws_id: Annotated[str, Depends(APIDependencies.param_ws_id)],
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
        data: Annotated[
            list[dict],
            Body(
                description="""
up to 10 dictionary created from GulpDocuments, dictionary must to be contains:
- event.original: the orginal log injest in gulp
- timestamp: allow to identify the timeline
- agent.type: allow AI to identify log agent""",
                max_length=10,
            ),
        ] = None,
    ) -> JSONResponse:
        params = locals()
        params.pop("self")
        ServerUtils.dump_params(params)
        try:
            # verify read permission
            user_id: int
            s: GulpUserSession
            async with GulpCollab.get_instance().session() as sess:
                s, _, _ = await GulpOperation.get_by_id_wrapper(
                    sess, token, operation_id, permission=GulpUserPermission.READ
                )
                user_id = s.user_id

            final_prompt = self._PROMPT.replace(
                "<%DATA%>",
                orjson.dumps(data, option=orjson.OPT_INDENT_2).decode(),
            )
            GulpServer.spawn_bg_task(
                self._call_with_retry(
                    operation_id=operation_id,
                    ws_id=ws_id,
                    req_id=req_id,
                    prompt=final_prompt,
                    user_id=user_id,
                )
            )
            return JSONResponse(JSendResponse.pending(req_id=req_id))
        except Exception as ex:
            raise JSendException(req_id=req_id) from ex

    async def _call_with_retry(
        self,
        operation_id: str,
        ws_id: str,
        req_id: str,
        prompt: str,
        user_id: int,
    ):
        # free model sometimes cannot return response and return error
        # implemented a retry mechanism to avoid temporary avaiability model

        max_retries = 5
        delay = 2.0
        last_exception = None
        for attempt in range(max_retries):
            try:
                await self._call_ai(
                    operation_id=operation_id,
                    ws_id=ws_id,
                    req_id=req_id,
                    prompt=prompt,
                    user_id=user_id,
                )
                break
            except RequestCanceledError as e:
                raise e
            except Exception as e:
                MutyLogger.get_instance().warning(
                    f"Ai response with error {e}\n retries number {attempt} after {delay} seconds...\n"
                )
                last_exception = e
                await asyncio.sleep(delay)
        if last_exception:
            raise last_exception

    async def _call_ai(
        self,
        operation_id: str,
        ws_id: str,
        req_id: str,
        user_id,
        prompt: str,
    ):
        url = "https://openrouter.ai/api/v1/chat/completions"
        headers = {
            "Authorization": f"Bearer {self._api_key}",
            "Content-Type": "application/json",
        }

        payload = {
            "model": self._model,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True,
        }
        async with GulpCollab.get_instance().session() as sess:
            if await GulpRequestStats.is_canceled(sess, req_id):
                raise RequestCanceledError()

            redis_broker = GulpRedisBroker.get_instance()
            async with aiohttp.ClientSession() as session:
                async with session.post(url, headers=headers, json=payload) as r:
                    if r.status != 200:
                        error_msg = await r.text()
                        MutyLogger.get_instance().error(
                            f"API Error: {r.status} - {error_msg}"
                        )
                        await redis_broker.put(
                            t="ai_assistant_error",
                            operation_id=operation_id,
                            ws_id=ws_id,
                            user_id=user_id,
                            d=error_msg,
                            req_id=req_id,
                        )
                        return

                    buffer = ""
                    async for chunk_bytes in r.content.iter_chunked(1024):
                        buffer += chunk_bytes.decode("utf-8")
                        while True:
                            try:
                                # Find the next complete SSE line
                                line_end = buffer.find("\n")
                                if line_end == -1:
                                    break

                                line = buffer[:line_end].strip()
                                buffer = buffer[line_end + 1 :]

                                if line.startswith("data: "):
                                    data = line[6:]
                                    if data == "[DONE]":
                                        await redis_broker.put(
                                            t="ai_assistant_done",
                                            operation_id=operation_id,
                                            ws_id=ws_id,
                                            user_id=user_id,
                                            req_id=req_id,
                                        )
                                        return

                                    try:
                                        data_obj = json.loads(data)
                                        content = data_obj["choices"][0]["delta"].get(
                                            "content"
                                        )
                                        if content:
                                            if await GulpRequestStats.is_canceled(
                                                sess, req_id
                                            ):
                                                raise RequestCanceledError()
                                            await redis_broker.put(
                                                t="ai_assistant_stream",
                                                operation_id=operation_id,
                                                ws_id=ws_id,
                                                user_id=user_id,
                                                req_id=req_id,
                                                d=content,
                                            )
                                    except json.JSONDecodeError:
                                        pass
                            except Exception:
                                break
        # Ensure we signal 'done' if the stream closes naturally without [DONE] token
        # or if an exception occurred, to unblock the client UI.
        await redis_broker.put(
            t="ai_assistant_done",
            operation_id=operation_id,
            ws_id=ws_id,
            user_id=user_id,
            req_id=req_id,
        )
