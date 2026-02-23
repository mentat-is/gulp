"""
Plugin: Open Telemetry receiver

This plugins implements an extension for Gulp that allow to ingest logs and trace
from open telemetry Collector

Reference:
https://github.com/open-telemetry/opentelemetry-proto/blob/main/docs/specification.md#otlphttp

"""

import asyncio
import gzip
import aiohttp
from fastapi import Body, Depends, HTTPException, Header, Query, Request
import json
from typing import Annotated, Any, Optional, override

from fastapi.responses import JSONResponse
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
import muty
import muty.string
import orjson
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.operation import GulpOperation
from gulp.api.collab.stats import (
    GulpIngestionStats,
    GulpRequestStats,
    RequestCanceledError,
    RequestStatsType,
)
from gulp.api.collab.structs import GulpRequestStatus, GulpUserPermission
from gulp.api.collab.user import GulpUser
from gulp.api.collab.user_session import GulpUserSession
from gulp.api.collab_api import GulpCollab
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.api.server.server_utils import ServerUtils
from gulp.api.server.structs import APIDependencies
from gulp.api.server_api import GulpServer
from gulp.api.ws_api import GulpRedisBroker
from gulp.config import GulpConfig
from gulp.plugin import GulpPluginBase, GulpPluginType

from muty.jsend import JSendException, JSendResponse
from muty.log import MutyLogger

from gulp.structs import GulpPluginParameters, ObjectNotFound


class Plugin(GulpPluginBase):

    basic_security = HTTPBasic(auto_error=False)

    @override
    def desc(self) -> str:
        """
        Summary:
        - args: none
        - does: return a short description of the plugin
        - returns: description string
        """
        return "Gulp Otel receivers, allow collectors to send logs/metrics/traces to a specific operation defined in the collector config"

    def type(self) -> GulpPluginType:
        return GulpPluginType.EXTENSION

    def display_name(self):
        return "Gulp Open Telemetry receivers"

    @override
    async def post_init(self, *kwargs):
        if self.is_running_in_main_process():
            GulpServer.get_instance().add_api_route(
                "/v1/logs",
                self.receive_otel_logs_handler,
                methods=["POST"],
                response_model=JSendResponse,
                response_model_exclude_none=True,
                tags=["otel"],
                responses={
                    200: {
                        "content": {
                            "application/json": {
                                "example": {
                                    "status": "success",
                                }
                            }
                        }
                    }
                },
                summary="endpoint to receive open telemetry logs",
                description="endpoint to receive open telemetry logs",
            )
            GulpServer.get_instance().add_api_route(
                "/v1/metrics",
                self.receive_otel_metrics_handler,
                methods=["POST"],
                response_model=JSendResponse,
                response_model_exclude_none=True,
                tags=["otel"],
                responses={
                    200: {
                        "content": {
                            "application/json": {
                                "example": {
                                    "status": "success",
                                }
                            }
                        }
                    }
                },
                summary="endpoint to receive open telemetry metrics",
                description="endpoint to receive open telemetry metrics",
            )
            GulpServer.get_instance().add_api_route(
                "/v1/traces",
                self.receive_otel_traces_handler,
                methods=["POST"],
                response_model=JSendResponse,
                response_model_exclude_none=True,
                tags=["otel"],
                responses={
                    200: {
                        "content": {
                            "application/json": {
                                "example": {
                                    "status": "success",
                                }
                            }
                        }
                    }
                },
                summary="endpoint to receive open telemetry traces",
                description="endpoint to receive open telemetry traces",
            )
        else:
            # Running in a worker process: log initialization for debugging only.
            MutyLogger.get_instance().debug("initialized in worker process")

    """ MIDDLEWARE """

    async def _authentication_service(
        request: Request,
        basic_creds: Optional[HTTPBasicCredentials] = Depends(basic_security),
        token: Annotated[str, Header(description="token")] = None,
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ) -> str:
        """
        Summary:
        - args: `request`, optional `basic_creds`, optional `token`, optional `req_id`
        - does: authenticate requests using either an `Operation-ID` token header or HTTP Basic creds
                If a `token` header is present it is returned. Otherwise performs user login
                with the provided HTTP Basic credentials and returns the session id.
        - returns: a string representing the token or user session id
        """
        # Prefer explicit token header when provided
        if token:
            return token
        # Fall back to HTTP Basic authentication if credentials were provided
        if basic_creds:
            async with GulpCollab.get_instance().session() as sess:
                ip: str = request.client.host if request.client else "unknown"
                s = await GulpUser.login(
                    sess,
                    user_id=basic_creds.username,
                    password=basic_creds.password,
                    req_id=req_id,
                    user_ip=ip,
                )
                return s.id
        # If neither method authenticated, return HTTP 401
        raise HTTPException(status_code=401, detail="invalid authentication")

    """ HANDLER"""

    async def receive_otel_logs_handler(
        self,
        request: Request,
        content_encoding: Annotated[
            str,
            Header(description="content enconding", alias="content-encoding"),
        ],
        token: Annotated[str, Depends(_authentication_service)],
        operation_id: Annotated[
            str,
            Header(
                alias="Operation-ID",
                description="the operation_id to use to ingest file",
            ),
        ],
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ):
        """
        Summary:
        - args: `request`, `content_encoding` header, authenticated `token`, `operation_id` header, optional `req_id`
        - does: parse the incoming OTLP logs payload, flatten records and enqueue ingestion as a background task
        - returns: a JSON success response or raises a `JSendException` on error
        """
        params = locals()
        params.pop("self")
        # parse request payload (handles gzip or plain json)
        payload: None
        payload = await self._get_payload(request, content_encoding)
        params["request"] = ""
        ServerUtils.dump_params(params)

        try:
            (user_session, operation) = await self._get_operation(token, operation_id)

            records: list[dict[str, Any]] = []
            resource_logs = payload.get("resourceLogs", [])
            records = self._get_flatten_logs(resource_logs)

            # Spawn background ingestion task to avoid blocking the HTTP request
            await GulpServer.get_instance().spawn_worker_task(
                self._ingest_ot_record,
                user_session.user_id,
                operation_id,
                operation.index,
                req_id,
                records,
                wait=True,
            )
            return JSONResponse(JSendResponse.success())

        except Exception as ex:
            # Convert to JSendException to follow API error format
            raise JSendException() from ex

    async def receive_otel_metrics_handler(
        self,
        request: Request,
        content_encoding: Annotated[
            str,
            Header(description="content enconding", alias="content-encoding"),
        ],
        token: Annotated[str, Depends(_authentication_service)],
        operation_id: Annotated[
            str,
            Header(
                alias="Operation-ID",
                description="the operation_id to use to ingest file",
            ),
        ],
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ):
        """
        Summary:
        - args: `request`, `content_encoding` header, authenticated `token`, `operation_id` header, optional `req_id`
        - does: parse OTLP metrics payload, flatten metrics and enqueue ingestion asynchronously
        - returns: a JSON success response or raises `JSendException` on error
        """
        params = locals()
        params.pop("self")
        payload: None
        payload = await self._get_payload(request, content_encoding)
        params["request"] = ""
        ServerUtils.dump_params(params)

        try:
            (user_session, operation) = await self._get_operation(token, operation_id)

            records: list[dict[str, Any]] = []
            resource_logs = payload.get("resourceMetrics", [])
            records = self._get_flatten_metrics(resource_logs)
            await GulpServer.get_instance().spawn_worker_task(
                self._ingest_ot_record,
                user_session.user_id,
                operation_id,
                operation.index,
                req_id,
                records,
                wait=True,
            )
            return JSONResponse(JSendResponse.success())

        except Exception as ex:
            raise JSendException() from ex

    async def receive_otel_traces_handler(
        self,
        request: Request,
        content_encoding: Annotated[
            str,
            Header(description="content enconding", alias="content-encoding"),
        ],
        token: Annotated[str, Depends(_authentication_service)],
        operation_id: Annotated[
            str,
            Header(
                alias="Operation-ID",
                description="the operation_id to use to ingest file",
            ),
        ],
        req_id: Annotated[str, Depends(APIDependencies.ensure_req_id_optional)] = None,
    ):
        """
        Summary:
        - args: `request`, `content_encoding` header, authenticated `token`, `operation_id` header, optional `req_id`
        - does: parse OTLP traces payload, flatten spans and enqueue ingestion asynchronously
        - returns: a JSON success response or raises `JSendException` on error
        """
        params = locals()
        params.pop("self")
        payload: None
        payload = await self._get_payload(request, content_encoding)
        params["request"] = ""
        ServerUtils.dump_params(params)

        try:
            (user_session, operation) = await self._get_operation(token, operation_id)

            records: list[dict[str, Any]] = []
            resource_logs = payload.get("resourceSpans", [])
            records = self._get_flatten_spans(resource_logs)

            await GulpServer.get_instance().spawn_worker_task(
                self._ingest_ot_record,
                user_session.user_id,
                operation_id,
                operation.index,
                req_id,
                records,
                wait=True,
            )
            return JSONResponse(JSendResponse.success())

        except Exception as ex:
            raise JSendException() from ex

    """ INGEST """

    async def _ingest_ot_record(
        self,
        user_id: str,
        operation_id: str,
        index: str,
        req_id: str,
        records: list[dict],
    ):
        """
        Summary:
        - args: `user_id`, `operation_id`, `index`, `req_id`, `records` (list of dicts)
        - does: create or obtain request stats, load the plugin instance and call its `ingest_raw`
                method in order to persist the provided records. Ensures plugin cleanup.
        - returns: None
        """

        ws_id = muty.string.generate_unique()
        async with GulpCollab.get_instance().session() as sess:
            # create (or get existing) stats
            stats: GulpRequestStats
            stats, _ = await GulpRequestStats.create_or_get_existing(
                sess,
                req_id,
                user_id,
                operation_id,
                ws_id=ws_id,
                never_expire=True,
                req_type=RequestStatsType.REQUEST_TYPE_RAW_INGESTION,
                data=GulpIngestionStats().model_dump(exclude_none=True),
            )

            # run plugin: dynamically load this plugin implementation and forward the chunk
            mod: GulpPluginBase = None
            try:
                mod = await GulpPluginBase.load("otel_receiver", True)
                await mod.ingest_raw(
                    sess,
                    stats,
                    user_id,
                    req_id,
                    ws_id,
                    index,
                    operation_id,
                    orjson.dumps(records),
                )
            except Exception as ex:
                MutyLogger.get_instance().exception(ex)
            finally:
                if mod:
                    # Ensure plugin flush and unload to release resources
                    await mod.update_final_stats_and_flush()
                    await mod.unload()

    """ INGESTION METHODS"""

    @override
    async def _record_to_gulp_document(
        self, record: dict, record_idx: int, **kwargs
    ) -> GulpDocument:
        """
        Summary:
        - args: `record` (flattened dict), `record_idx` (sequence index), `**kwargs` passed to `_process_key`
        - does: convert a flattened OTLP record into a `GulpDocument` by processing keys
        - returns: a `GulpDocument` instance ready for ingestion
        """

        # extract timestamp from known fields with fallback
        time_str = record.pop("timeUnixNano", "")
        if not time_str:
            time_str = record.pop("observedTimeUnixNano", "")

        # original event payload (used as the raw message) with fallback to JSON string
        event_original = record.pop("event_original", "")
        if not event_original:
            event_original = json.dumps(record)

        d: dict = {}
        # map/transform each key/value using shared processing logic in `_process_key`
        for k, v in record.items():
            mapped = await self._process_key(k, v, d, **kwargs)
            d.update(mapped)

        # create and return the GulpDocument
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            event_original=event_original,
            event_sequence=record_idx,
            timestamp=time_str,
            **d,
        )

    @override
    async def ingest_raw(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        chunk: bytes,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        last: bool = False,
        **kwargs,
    ) -> GulpRequestStatus:
        """
        Summary:
        - args: `sess`, `stats`, `user_id`, `req_id`, `ws_id`, `index`, `operation_id`, `chunk` (bytes),
                 optional `flt`, `plugin_params`, `last`, and additional kwargs
        - does: ensure plugin mapping parameters, call base class ingestion, decode the chunk
                 into records and process each record via `process_record`
        - returns: current `GulpRequestStatus` from `stats`
        """

        js: list[dict] = []
        # ensure the plugin mapping parameters are set for OTEL mapping
        plugin_params = GulpPluginParameters()
        plugin_params.mapping_parameters.mapping_file = "otel.json"
        plugin_params.mapping_parameters.mapping_id = "default"
        plugin_params = self._ensure_plugin_params(plugin_params)

        # call parent implementation for shared pre-processing
        await super().ingest_raw(
            sess,
            stats,
            user_id,
            req_id,
            ws_id,
            index,
            operation_id,
            chunk,
            flt=flt,
            plugin_params=plugin_params,
            last=last,
            **kwargs,
        )

        # chunk is JSON bytes representing a list of flattened records
        js = orjson.loads(chunk.decode("utf-8"))

        # iterate and process each record sequentially
        doc_idx: int = 0
        for rr in js:
            if not await self.process_record(rr, doc_idx, flt=flt):
                break
            doc_idx += 1
        return stats.status

    """ HELPER """

    async def _get_payload(self, request, content_encoding):
        try:
            # read raw bytes from the request and optionally decompress gzip
            body_bytes = await request.body()
            if content_encoding != "json":
                body_bytes = gzip.decompress(body_bytes)

            # parse JSON into a Python object
            payload = json.loads(body_bytes)
            MutyLogger.get_instance().debug(f"payload:\n-------\n{payload}\n----------")
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
            raise JSendException() from ex
        return payload

    async def _get_operation(
        self, token, operation_id
    ) -> tuple["GulpUserSession", "GulpOperation"]:
        async with GulpCollab.get_instance().session() as sess:
            s: GulpUserSession
            operation: GulpOperation
            try:
                # retrieve operation by id (may raise ObjectNotFound). If it exists, return it.
                s, operation, _ = await GulpOperation.get_by_id_wrapper(
                    sess, token, operation_id, permission=GulpUserPermission.INGEST
                )
            except ObjectNotFound as ex:
                # If the operation does not exist yet, validate the token and create a new operation
                s: GulpUserSession = await GulpUserSession.check_token(
                    sess, token, permission=GulpUserPermission.INGEST
                )
                operation = await GulpOperation.create_operation(
                    sess=sess, name=operation_id, user_id=s.user.id
                )
            return (s, operation)

    def _get_flatten_logs(self, resource_logs: list[dict]) -> list[dict[str, Any]]:
        """
            The function expects OTLP log JSON structured as a list of `resourceLogs`.
            Each `resourceLog` contains a `resource` object (holding resource-level attributes) and `scopeLogs`.
            Each `scopeLog` contains a `logRecords` list with individual `logRecord` objects (their `body`, `attributes`, timestamps, and other fields).

            For each `resourceLog` the code flattens resource attributes with `_flatten_attributes` and computes a `gulp.context_name` via `_get_context_name`.
            It then iterates each `scopeLog` and each `logRecord`, extracting `body` and `attributes`, flattening both (the body with `_get_body_values` and attributes via
            `_flatten_attributes`) and merging them with the base `log` dict and resource-level attributes. The merge produces a single-level dict
            representing the full enriched event. The function also computes a `gulp.source_name` using `_get_gulp_source` so that downstream
            mappings can route the document appropriately.

        RESULTS :
            Returns a list of flattened dictionaries, each representing a
            log-ready document. Each dict contains merged keys from resource,
            log, attributes and body values, and includes keys like
            `timeUnixNano`, `event_original`, `gulp.context_name` and
            `gulp.source_name`.
        """

        results = []

        for r_log in resource_logs:
            # resource-level metadata (service.name, host, etc.)
            resource = r_log.get("resource", {})
            # flatten resource attributes into a simple dict for merging
            resource_flatten = self._flatten_attributes(resource.get("attributes", []))
            # compute a context/name for these logs based on resource attrs
            gulp_context = self._get_context_name(resource_flatten)

            scope_logs = r_log.get("scopeLogs", [])
            for scope_log in scope_logs:
                logs = scope_log.get("logRecords")
                for log in logs:
                    # Extract the structured body (may be a kvlistValue or primitive)
                    # and the record attributes. We pop them from `log` to avoid
                    # keeping nested structures and then flatten both.
                    body = log.pop("body", None)
                    attributes = log.pop("attributes", [])
                    body_flatten = self._get_body_values(body)
                    log_flatten = self._flatten_attributes(attributes)

                    # Merge resource-level, log-level and body/attribute-derived
                    # key/values into a single document that mapping expects.
                    record = resource_flatten | log | log_flatten | body_flatten
                    record["gulp.context_name"] = gulp_context
                    # Determine a reasonable source name (file, service, unit, ...)
                    gulp_source = self._get_gulp_source("log", record)
                    record["gulp.source_name"] = gulp_source

                    results.append(record)

        return results

    def _get_flatten_metrics(
        self, resource_metrics: list[dict]
    ) -> list[dict[str, Any]]:
        """
            OTLP metrics arrive as `resourceMetrics` where each item contains a `resource` dict and `scopeMetrics`.
            Each `scopeMetric` includes one or more `metrics`, and each `metric` contains typed payloads (e.g. `gauge`, `sum`, `histogram`, `summary`)
            that expose `dataPoints`.

            The function iterates each resource and scope, flattens resource attributes and computes context/source.
            For each metric it determines the metric type and extracts its `dataPoints`.
            Each datapoint is flattened (labels/attributes) and combined with the resource-level attributes.
            Datapoints that share the same timestamp/context/source/scope are grouped into a single document
            (using `group_key`) — this reduces the number of documents by aggregating metrics that belong together in time and context.
            Each grouped document contains a `metrics` list with per-datapoint records (with `metric_name`, `metric_type`, value fields, etc.).

        RESULTS:
            Returns a list of grouped metric documents.
            Each document has top-level metadata (`timeUnixNano`, `gulp.context_name`,`gulp.source_name`, `event`)
            and a `metrics` array containing the flattened datapoint records ready for ingestion.
        """

        grouped_docs = {}
        for resource_metric in resource_metrics:
            # retrieve resource information to enrich metrics and compute context/source
            resource = resource_metric.get("resource", {})
            resource_flatten = self._flatten_attributes(resource.get("attributes", []))
            gulp_context = self._get_context_name(resource_flatten)
            gulp_source = self._get_gulp_source("metric", resource_flatten)

            scope_metrics = resource_metric.get("scopeMetrics", [])
            for scope_metric in scope_metrics:
                scope_name = scope_metric.get("scope", {}).get("name", "unknown_scope")

                for metric in scope_metric.get("metrics", []):
                    metric_name = metric.get("name")
                    metric_type = "unknown"
                    data_points = []
                    # Determine where datapoints live depending on metric type
                    if "gauge" in metric:
                        data_points = metric["gauge"].get("dataPoints", [])
                        metric_type = "gauge"
                    elif "sum" in metric:
                        data_points = metric["sum"].get("dataPoints", [])
                        metric_type = "counter"
                    elif "histogram" in metric:
                        # histograms may encode buckets; we extract datapoints
                        data_points = metric.get("histogram", {}).get("dataPoints", [])
                        metric_type = "histogram"
                    elif "summary" in metric:
                        data_points = metric.get("summary", {}).get("dataPoints", [])
                        metric_type = "summary"

                    for dp in data_points:
                        # Use the datapoint timestamp as a primary grouping key so
                        # that multiple metrics from the same resource/scope at
                        # the same instant are combined into one document.
                        timestamp = dp.pop("timeUnixNano")

                        # group_key collates scope, context and source with the timestamp
                        group_key = (
                            f"{timestamp}_{scope_name}-{gulp_context}_{gulp_source}"
                        )
                        if group_key not in grouped_docs:
                            # Create a new grouped document that will hold
                            # multiple metric datapoints for this instant/context
                            grouped_docs[group_key] = {
                                "timeUnixNano": timestamp,
                                "gulp.context_name": gulp_context,
                                "gulp.source_name": gulp_source,
                                "event": {"kind": "metric", "dataset": scope_name},
                                "metrics": [],  # container for aggregated metric datapoints
                            }
                        doc = grouped_docs[group_key]

                        # Flatten datapoint attributes (labels) and merge with
                        # resource-level attributes and datapoint fields
                        dp_flatten = self._flatten_attributes(dp.pop("attributes", []))
                        record = resource_flatten | dp_flatten | dp
                        record["metric_type"] = metric_type
                        record["metric_name"] = metric_name
                        record["timeUnixNano"] = timestamp

                        doc["metrics"].append(record)

        return list(grouped_docs.values())

    def _get_flatten_spans(self, resource_spans: list[dict]) -> list[dict[str, Any]]:
        """
            OTLP traces are represented as `resourceSpans`.
            Each `resourceSpan` has a `resource` (attributes) and `scopeSpans`.
            Each `scopeSpan` contains a `spans` list where each span includes timing (start/end), attributes, events and other trace-specific fields.

            The function flattens resource attributes and computes a context name.
            For every span it pops the `startTimeUnixNano` to use as the document timestamp, flattens span attributes and merges resource, span fields and flattened attributes into a single record.
            It sets `timeUnixNano`, `gulp.context_name`, and determines a `gulp.source_name` via `_get_gulp_source`.

        RESULTS
            Returns a list of flattened span documents that include span metadata (trace/span ids, names), timing (`timeUnixNano`) and
            enriched context/source information suitable for ingestion.
        """

        results = []

        for r_span in resource_spans:
            # Resource-level attributes for the span (service, environment, ...)
            resource = r_span.get("resource", {})
            resource_flatten = self._flatten_attributes(resource.get("attributes", []))
            gulp_context = self._get_context_name(resource_flatten)

            scope_logs = r_span.get("scopeSpans", [])
            for scope_span in scope_logs:
                spans = scope_span.get("spans", [])
                for span in spans:
                    # Use start time as the event timestamp for ingestion.
                    # We `pop` it from `span` to avoid duplicating nested
                    # structures — the start time becomes `timeUnixNano` on the
                    # resulting document.
                    timestamp = span.pop("startTimeUnixNano")
                    # Flatten span attributes (e.g. db.system, messaging.system)
                    span_attr = self._flatten_attributes(span.pop("attributes", []))
                    # Merge resource-level, span fields and flattened attrs
                    record = resource_flatten | span | span_attr
                    record["timeUnixNano"] = timestamp
                    record["gulp.context_name"] = gulp_context
                    # Determine a meaningful source for the span
                    gulp_source = self._get_gulp_source("span", span_attr)
                    record["gulp.source_name"] = gulp_source
                    results.append(record)
        return results

    def _get_body_values(self, body_obj: dict) -> dict:
        body_attributes = {}
        message = ""
        if "kvlistValue" in body_obj:
            values = body_obj["kvlistValue"].get("values", [])
            for item in values:
                key = item.get("key")
                val_obj = item.get("value", {})
                val_clean = self._safe_value(val_obj)

                if key == "MESSAGE":
                    message = str(val_clean)
                elif key:
                    body_attributes[key] = val_clean

            # message setting fallback
            if not message:
                message = json.dumps(body_attributes)

            body_attributes["event_original"] = str(message)
        # body have message value, set it
        else:
            # when body is a primitive value, use its safe representation as the original event
            body_attributes["event_original"] = str(self._safe_value(body_obj))

        return body_attributes

    def _get_timestamp(self, log_records: dict) -> str:
        if log_records.get("timeUnixNano"):
            return log_records["timeUnixNano"]
        if log_records.get("observedTimeUnixNano"):
            return log_records["observedTimeUnixNano"]
        raise ValueError("cannot extract timestamp value")

    def _flatten_attributes(self, attributes: list[dict[str, Any]]) -> dict[str, Any]:
        result = {}
        try:
            for attr in attributes:
                key = attr.get("key")
                val_obj = attr.get("value", {})
                result[key] = self._safe_value(val_obj)
            return result
        except Exception as ex:
            # Log and return an empty dict if attribute parsing fails
            MutyLogger.get_instance().error(
                "error during parse attributes for {attributes}, error: {ex}"
            )
            return {}

    def _safe_value(self, value_obj: dict) -> Any:
        if not value_obj:
            return None
        # log value
        if "stringValue" in value_obj:
            return value_obj["stringValue"]
        if "intValue" in value_obj:
            return int(value_obj["intValue"])
        if "boolValue" in value_obj:
            return bool(value_obj["boolValue"])
        if "doubleValue" in value_obj:
            return float(value_obj["doubleValue"])
        if "arrayValue" in value_obj:
            # recursively resolve array elements
            return [
                self._safe_value(v) for v in value_obj["arrayValue"].get("values", [])
            ]
        # metric value
        if "asDouble" in value_obj:
            return value_obj["asDouble"]
        elif "asInt" in value_obj:
            return int(value_obj["asInt"])
        return str(value_obj)

    def _get_gulp_source(self, signal_type: str, flattened_attrs: dict) -> str:
        """
        signal_type: "log", "metric", "span"
        flattened_attrs: dict (Resource + Log/Span/Metric)
        """

        # 0. Highest priority: allow a forced `gulp.source` provided by the collector
        if flattened_attrs.get("gulp.source"):
            return flattened_attrs.pop("gulp.source")

        # 1. Logic for LOGS
        if signal_type == "log":
            return (
                flattened_attrs.get("log.file.path", "")
                or flattened_attrs.get("log.file.name", "")
                or flattened_attrs.get("systemd.unit", "")
                or flattened_attrs.get("syslog.identifier", "")
                or flattened_attrs.get("container.name", "")
                or flattened_attrs.get("service.name", "")
                or flattened_attrs.get("SYSLOG_IDENTIFIER", "")
                or "unknown_log_source"
            )

        # 2. Logic for METRICS
        elif signal_type == "metric":
            return flattened_attrs.get("service.name", "") or "system_metrics"

        # 3. Logic for SPANS (traces)
        elif signal_type == "span":
            # If the span is for a DB or messaging system prefer that identifier
            db_system = flattened_attrs.get("db.system")
            if db_system:
                return db_system

            msg_system = flattened_attrs.get("messaging.system")
            if msg_system:
                return msg_system
            return flattened_attrs.get("service.name") or "unknown_trace_source"

        return "unknown_source"

    def _get_context_name(self, resource_flatten):
        if resource_flatten.get("gulp.context"):
            return resource_flatten.pop("gulp.context")

        return (
            resource_flatten.get("host.name")
            or resource_flatten.get("k8s.pod.name")
            or resource_flatten.get("host.id")
            or "unknown_context"
        )
