"""Gulp global definitions.

This module contains core data structures and exceptions used throughout Gulp.
It defines models for API parameters, plugin configuration, and common utility classes.

Key components:
- Exception classes for object management
- API parameter and method models
- Plugin parameter handling
- Sorting and configuration enums
"""

from enum import StrEnum
from typing import Annotated, Any, Literal, Optional, Protocol, TYPE_CHECKING

from muty.log import MutyLogger
from muty.pydantic import autogenerate_model_example_by_class
import muty.time
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.mapping.models import GulpMapping, GulpSigmaMapping
from gulp.api.opensearch.filters import GulpIngestionFilter

if TYPE_CHECKING:
    from gulp.plugin import GulpPluginBase


class ObjectAlreadyExists(Exception):
    pass


class ObjectNotFound(Exception):
    pass


class GulpAPIParameter(BaseModel):
    """
    describes a parameter for a Gulp API method.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "name": "ignore_mapping",
                    "type": "bool",
                    "default_value": False,
                    "desc": "ignore mapping file and leave the field as is.",
                    "required": True,
                }
            ]
        }
    )

    name: Annotated[str, Field(description="the parameter.")]
    type: Annotated[
        Literal["bool", "str", "int", "float", "dict", "list"],
        Field(description="parameter type."),
    ]
    default_value: Annotated[Optional[Any], Field(description="default value.")] = None
    desc: Annotated[Optional[str], Field(description="parameter description.")] = None
    location: Annotated[
        Optional[Literal["query", "header", "body"]],
        Field(description="where the parameter is located, for API requests."),
    ] = "query"
    required: Annotated[
        bool, Field(False, description="is the parameter required ?")
    ] = False
    example: Annotated[
        Optional[Any], Field(description="an example value for the parameter, if any.")
    ] = None


class GulpAPIMethod(BaseModel):
    """
    describes a Gulp API method.
    """

    method: Annotated[
        Literal["PUT", "GET", "POST", "DELETE", "PATCH"],
        Field(description="the method to be used"),
    ]
    url: Annotated[
        str, Field(..., description="the endpoint url, relative to the base host")
    ]
    params: Annotated[
        list[GulpAPIParameter],
        Field(description="list of parameters for the method"),
    ] = Field(default_factory=list)
    


class GulpMappingParameters(BaseModel):
    """
    describes mapping parameters for API methods.
    - `mapping_file` and `additional_mapping_files` are used to load mappings from files.
    - `mappings` is used to pass a dictionary of mappings directly.
    - `mapping_id` is used to select a specific mapping from the file or dictionary.
    """

    model_config = ConfigDict(
        json_schema_extra={
            "examples": [
                {
                    "mapping_file": "mftecmd_csv.json",
                    "mappings": {
                        "the_mapping_id": autogenerate_model_example_by_class(
                            GulpMapping
                        ),
                    },
                    "mapping_id": "record",
                }
            ]
        }
    )

    # mapping file name in the mapping files directory (main or extra) to read GulpMapping entries from
    mapping_file: Annotated[
        Optional[str],
        Field(
            description=(
                "mapping file name in the mapping files directory (main or extra) to read `GulpMapping` entries from. (if `mappings` is set, this is ignored).\n"
                "- `mappings` is ignored if this is set.\n"
                "- `additional_mapping_files` and `additional_mappings` can be used to load further mappings from other files or directly from a dictionary."
            ),
        ),
    ] = None

    # the GulpMapping to select in mapping_file or mappings object
    mapping_id: Annotated[
        Optional[str],
        Field(
            description="the `GulpMapping` to select in `mapping_file` or `mappings` object: if not set, the first found GulpMapping is used.",
        ),
    ] = None

    # a dictionary of one or more { mapping_id: GulpMapping } to use directly
    mappings: Annotated[
        dict[str, GulpMapping],
        Field(
            description=(
                "a dictionary of one or more { mapping_id: GulpMapping } to use directly.\n"
                "- `mapping_file`, `additional_mapping_files`, `additional_mappings` are ignored if this is set."
            ),
        ),
    ] = Field(default_factory=dict)

    # specify further mappings from other mapping files
    additional_mapping_files: Annotated[
        list[tuple[str, str]],
        Field(
            description=(
                "if this is set, it allows to specify further mappings from other mapping files.\n"
                "each tuple is defined as (other_mapping_file, mapping_id): each `mapping_id` from `other_mapping_file` will be loaded and merged to the mappings identified by `mapping_id` selected during parsing of the **main** `mapping_file`."
            ),
        ),
    ] = Field(default_factory=list)

    # pass additional mappings as a dictionary
    additional_mappings: Annotated[
        dict[str, GulpMapping],
        Field(
            description=(
                "same as `additional_mapping_files`, but used to pass additional mappings as a dictionary of { mapping_id: GulpMapping }.\n"
                "each `mapping_id` GulpMapping defined will be merged to the mappings identified by `mapping_id` selected during parsing of the **main** `mapping_file`."
            ),
        ),
    ] = Field(default_factory=dict)

    # internal use, only for sigma queries
    sigma_mappings: Annotated[
        dict[str, GulpSigmaMapping],
        Field(
            description=(
                "internal use, only for sigma queries: if set, rules to map `logsource` in sigma rules when using the mapping previously stored for each GulpSource.\n"
                'each key corresponds to `logsource.service` in the sigma rule: basically, we want to use the sigma rule only if a (mapped) "logsource.service" is defined in the sigma rule (or no `logsource` is defined at all in the sigma rule).'
            ),
        ),
    ] = Field(default_factory=dict)

    def is_empty(self) -> bool:
        """
        check if mapping parameters are empty.

        a GulpMappingParameters is considered empty if all of the following are true:

        - `mappings` is empty
        - `mapping_file` is None or empty
        - `sigma_mappings` is empty
        - `mapping_id` is None or empty
        - `additional_mapping_files` is empty
        - `additional_mappings` is empty

        Returns:
            bool: True if all parameters are None, False otherwise
        """
        if (
            not self.mappings
            and not self.mapping_file
            and not self.sigma_mappings
            and not self.mapping_id
            and not self.additional_mapping_files
            and not self.additional_mappings
        ):
            # no mappings at all
            MutyLogger.get_instance().warning("mapping parameters are empty")
            return True

        return False

    def _stringify(self) -> tuple:
        return (
            str(self.mapping_file)
            + str(self.mapping_id)
            + str(self.mappings)
            + str(self.additional_mapping_files)
            + str(self.additional_mappings)
            + str(self.sigma_mappings)
        )

    def __eq__(self, other):
        if not isinstance(other, GulpMappingParameters):
            return NotImplemented
        return self._stringify() == other._stringify()

    def __hash__(self):
        return hash(self._stringify())


class GulpProgressCallback(Protocol):
    """
    callback protocol for generic progress updates
    """

    async def __call__(
        self,
        sess: AsyncSession,
        total: int,
        current: int,
        req_id: str,
        last: bool = False,
        **kwargs,
    ) -> None:
        """
        callback function to report progress.

        Args:
            sess (AsyncSession): the current database session
            total (int): total number of items to process
            current (int): current number of processed items
            last (bool, optional): True if this is the last progress update. Defaults to False.
            req_id (str): originating request id
            **kwargs: additional arguments passed to the callback

        Returns:
            None
        """
        ...


class GulpDocumentsChunkCallback(Protocol):
    """
    callback protocol for chunk processing
    """

    async def __call__(
        self,
        sess: AsyncSession,
        chunk: list[dict],
        chunk_num: int = 0,
        total_hits: int = 0,
        index: str = None,
        last: bool = False,
        req_id: str = None,
        q_name: str = None,
        q_group: str = None,
        **kwargs,
    ) -> list[dict]:
        """
        callback function to process a chunk of documents.

        Args:
            sess (AsyncSession): the current database session
            chunk (list[dict]): one or more GulpDocument dictionaries
            chunk_num (int): current chunk number (starting from 0)
            total_hits (int): total number of hits for the query
            index (str|None): the index (may be different from operation_id), if any. Defaults to None.
            last (bool): True if this is the last chunk. Defaults to False.
            req_id (str|None): the originating request id, if any. Defaults to None.
            q_name (str|None): query name, if any. Defaults to None.
            q_group (str|None): query group, if any. Defaults to None.
            **kwargs: additional arguments passed to the callback
        Returns:
            list[dict]: the processed chunk of documents
        """
        ...
        ...

class GulpPluginParameters(BaseModel):
    """
    parameters for a plugin, to be passed to ingest and query_external API.

    additional custom parameters defined in GulpPlugin.custom_parameters may be added to the "model_extra" field, they will be passed to the plugin as is.
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "mapping_parameters": autogenerate_model_example_by_class(
                        GulpMappingParameters
                    ),
                    "override_chunk_size": 1000,
                    "custom_parameters": {
                        "some_custom_param": "some_value",
                        "some_custom_param_2": "some_value_2",
                    },
                }
            ]
        },
    )
    mapping_parameters: Annotated[
        Optional[GulpMappingParameters],
        Field(
            description="mapping parameters for the plugin.",
        ),
    ] = Field(default_factory=GulpMappingParameters)
    override_chunk_size: Annotated[
        Optional[int],
        Field(
            description="""this is used to override, on a request basis, the bufferized size of chunk before flushing to OpenSearch and possibly send to websocket.
            by default, this is set as configuration 'ingestion_documents_chunk_size' and can be overridden here i.e. when OpenSearch or websocket complains about too big chunks.""",
        ),
    ] = None
    override_allow_unmapped_fields: Annotated[
        bool,
        Field(
            description="""overrides, on a request basis, `ingestion_allow_unmapped_fields` in the configuration, to disable `gulp.unmapped` values generations during ingestion (default=True=allow).
            read the configuration documentation for more details (disabling may affect sigma rules matching)'
            """,
        ),
    ] = True
    timestamp_offset_msec: Annotated[
        int,
        Field(
            description="if not 0, this is used to offset document `@timestamp` (and `gulp.timestamp`) by the given number of milliseconds (positive or negative).",
        ),
    ] = 0
    custom_parameters: Annotated[
        dict,
        Field(
            description="additional plugin-specific custom parameters.",
        ),
    ] = Field(default_factory=dict)
    preview_mode: Annotated[
        bool,
        Field(
            description="if True, the plugin should run in preview mode (return synchronously a chunk of data)"
        ),
    ] = False

    _chunk_ingestion_callback: Annotated[
        GulpDocumentsChunkCallback,
        Field(
            description="internal use: callback to be set internally (i.e. by an extension plugin implementing `ingest_raw`) to process documents chunk AFTER being ingested in OpenSearch, ignored in preview mode.",
        ),
    ] = None

    store_file: Annotated[
        bool,
        Field(
            description="if True, and if this is a **file** ingestion operation, the file will be stored on the configured storage server prior to be ingested")
    ] = False

    def is_empty(self) -> bool:
        """
        check if **ALL** plugin parameters are empty.

        Returns:
            bool: True if all parameters are None/empty, False otherwise
        """
        if (
            self.mapping_parameters.is_empty()
            and not self.custom_parameters
            and not self.override_chunk_size
            and not self.timestamp_offset_msec
            and not self.store_file
        ):
            return True
        return False


class GulpPluginCustomParameter(GulpAPIParameter):
    """
    this is used **by the UI only** through the `plugin_list` API, which calls each plugin `custom_parameters()` entrypoint to get custom parameters name/type/description/default if defined.

    to pass custom parameters to a plugin via GulpPluginParameters, just use the `name` field as the key in the `GulpPluginParameters.custom_parameters` dictionary:

    ~~~js
    {
        // example GulpPluginParameters
        "mapping_file": "mftecmd_csv.json",
        "mappings": { "record": { "fields": { "timestamp": { "type": "datetime" } } } },
        "mapping_id": "record",
        "custom_parameters": {
            "some_custom_param": "some_value",
            "some_custom_param_2": "some_value_2"
        }
    }
    ~~~

    after `_initialize()` is called, the custom parameters will be available in `self._plugin_params.custom_parameters` field.

    ~~~python
    v = self._plugin_params.custom_parameters["some_custom_param"]
    ~~~
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "name": "ignore_mapping",
                    "type": "bool",
                    "default_value": False,
                    "desc": "ignore mapping file and leave the field as is.",
                    "required": True,
                }
            ]
        },
    )


class GulpSortOrder(StrEnum):
    """
    specifies the sort types for API accepting the "sort" parameter
    """

    ASC = "asc"
    DESC = "desc"


class GulpInternalEvent(BaseModel):
    """
    Gulp internal event, broadcasted by engine to plugins registered via GulpPluginEventQueues.register()
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "event_type": "login",
                    "timestamp_msec": 123456789,
                    "user_id": "user123",
                    "operation_id": "op123",
                    "data": {"ip": "192.168.2.10"},
                }
            ]
        },
    )
    type: Annotated[
        str,
        Field(
            description="The type of the event (e.g. login, logout, ingestion, etc.).",
        ),
    ]
    timestamp_msec: Annotated[
        int,
        Field(
            description="The timestamp of the event in milliseconds since epoch.",
        ),
    ]
    data: Annotated[
        dict,
        Field(
            description="Arbitrary data for the event.",
        ),
    ] = {}
    user_id: Annotated[
        Optional[str],
        Field(
            description="The user id associated with the event.",
        ),
    ] = None
    operation_id: Annotated[
        Optional[str],
        Field(
            description="The operation id associated with the event, if applicable.",
        ),
    ] = None
    req_id: Annotated[
        Optional[str],
        Field(
            description="The request id associated with the event, if applicable.",
        ),
    ] = None


class GulpInternalEventResult(BaseModel):
    """
    the result of an internal event callback, including the plugin that handled the event, the event type and the callback return value
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "plugins": ["win_evtx"],
                    "event": "chunk_post_ingest",
                    "result": {"modified_chunk": [{"field1": "value1"}, {"field1": "value2"}]},
                }
            ]
        },
    )
    plugins: Annotated[
        str|list[str],
        Field(description="The plugin or plugins that handled the event."),
    ]
    event: Annotated[
        str,
        Field(description="The type of the event."),
    ]
    result: Annotated[
        dict,
        Field(description="The return value of the callback, if any."),
    ] = None
    stop: Annotated[bool,
        Field(
            False,
            description="Whether to stop processing the event with other plugins (i.e. if True, the callback return value will be returned as final result of the event, without calling other plugins' callbacks).",
        ),
    ] = False


class GulpIngestSourceDoneInternalEvent(BaseModel):
    """
    this is sent  at the end of each source ingestion in GulpInternalEvent.data by the engine to plugins registered to the GulpInternalEventsManager.EVENT_INGEST event
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "plugin": "win_evtx",
                    "user_id": "user123",
                    "operation_id": "op123",
                    "req_id": "req123",
                    "file_path": "/path/to/file.evtx",
                    "plugin_params": {"param1": "value1", "param2": 2},
                    "errors": [],
                    "records_ingested": 1000,
                    "records_failed": 10,
                    "records_skipped": 5,
                    "records_processed": 1015,
                }
            ]
        },
    )
    plugin: Annotated[
        str, Field(description="The plugin (internal) name performing the ingestion.")
    ]
    user_id: Annotated[str, Field(description="The user id performing the ingestion.")]
    operation_id: Annotated[
        str, Field(description="The operation id associated with this ingestion.")
    ]
    req_id: Annotated[str, Field(description="The request id.")]
    file_path: Annotated[str, Field(description="The file path being ingested.")]
    status: Annotated[
        str,
        Field(
            description='The ingestion status, "done", "failed", "canceled".',
        ),
    ]
    plugin_params: Annotated[
        dict, Field(description="The plugin parameters used for the ingestion.")
    ]
    errors: Annotated[
        list[str],
        Field(description="A list of errors encountered during ingestion."),
    ] = []
    records_ingested: Annotated[
        int, Field(description="The total number of records ingested.")
    ] = 0
    records_failed: Annotated[
        int,
        Field(
            description="The total number of records that failed to be ingested.",
        ),
    ] = 0
    records_skipped: Annotated[
        int,
        Field(
            description="The total number of records skipped during ingestion.",
        ),
    ] = 0
    records_processed: Annotated[
        int, Field(description="The total number of records processed.")
    ] = 0


class GulpChunkPrePostIngestInternalEvent(BaseModel):
    """
    this is sent before/after each chunk ingestion by the engine to plugins registered to the GulpInternalEventsManager.EVENT_CHUNK_PRE/POST_INGEST event

    NOTE: EVENT_CHUNK_PRE_INGEST allows plugins to modify the chunk before ingestion (by returning a modified chunk in the callback, which is run synchronous by the engine)
    """

    model_config = ConfigDict(
        extra="allow",
        json_schema_extra={
            "examples": [
                {
                    "plugin": "win_evtx",
                    "user_id": "user123",
                    "operation_id": "op123",
                    "req_id": "req123",
                    "chunk": [{"field1": "value1"}, {"field1": "value2"}],
                }
            ]
        },
    )
    chunk: Annotated[
        list[dict], Field(description="The chunk of data being ingested.")
    ]
    index: Annotated[
        str, Field(description="The index the chunk is being ingested to.")
    ]
    operation_id: Annotated[
        str, Field(description="The operation id associated with this ingestion.")
    ]
    req_id: Annotated[
        str, Field(description="The request id associated with this ingestion.")
    ]
    ws_id: Annotated[
        str, Field(description="The WebSocket id associated with this ingestion.")
    ]
    user_id: Annotated[
        str, Field(description="The user id associated with this ingestion.")
    ]
    plugin: Annotated[
        str, Field(description="The plugin performing the ingestion.")
    ]
    flt: Annotated[
        Optional[GulpIngestionFilter], Field(description="The ingestion filter associated with this ingestion.")
    ] = None


class GulpInternalEventsManager:
    """
    Singleton class to manage internal events

    internal events are broadcasted by the engine to registered plugins.

    a plugin registers to receive internal events by calling GulpInternalEventsManager.register(plugin, types) where `types` is a list of event types the plugin is interested in.

    when an event is broadcasted (by core itself or by a plugin, calling GulpInternalEventsManager.dispatch_internal_event), core calls the `internal_event_callback` method of each registered plugin that is interested in the event type.
    """

    _instance: "GulpInternalEventsManager" = None

    # these events are broadcasted by core itself to registered plugins
    # further events may be added by plugins through register(): when calling dispatch_internal_event(), only plugins registered to receive the specific event type will receive it.

    # an user logged in
    EVENT_LOGIN: str = "user_login"  # data=GulpUserAccessPacket
    # an user logged out
    EVENT_LOGOUT: str = "user_logout"  # data=GulpUserAccessPacket
    # ingestion of a source has been completed
    EVENT_SOURCE_INGESTED: str = "ingest_source_done"  # data=GulpIngestSourceDoneInternalEvent
    # an operation is deleted
    EVENT_DELETE_OPERATION: str = "delete_operation"  # data= {"index": index}
    # a chunk of documents has been ingested and pushed to the websocket
    # data is a `GulpChunkPrePostIngestInternalEvent`
    EVENT_CHUNK_POST_INGEST: str = "chunk_post_ingest"
    # a chunk of documents is about to be ingested: this event is SYNCHRONOUS (backend wait for its processing)
    # data is a `GulpChunkPrePostIngestInternalEvent`, callback returns a GulpInternalEventResult with the modified chunk (if applicable)
    EVENT_CHUNK_PRE_INGEST: str = "chunk_pre_ingest"

    def __init__(self):
        self._initialized: bool = True

        # every dict have a "plugin" key which is a GulpPluginBase instance, and a "types" key with a list of event types this plugin is interested in
        self._plugins: dict[str, dict] = {}

    def __new__(cls):
        """
        Create a new instance of the class.
        """
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    @classmethod
    def get_instance(cls) -> "GulpInternalEventsManager":
        """
        get the manager instance.

        Returns:
            GulpInternalEventsManager: The internal events manager instance.
        """
        if not cls._instance:
            cls._instance = cls()
        return cls._instance

    def clear(self):
        """
        Clear the internal events manager plugins list
        """
        self._plugins = {}

    def register(self, plugin: "GulpPluginBase", types: list[str] = None) -> None:
        """
        Register a plugin to receive internal events.

        core will then call the plugin's `internal_event_callback` method when an event of the specified types is broadcasted with `dispatch_internal_event`.

        Args:
            plugin (GulpPluginBase): The plugin to register.
            types (list[str], optional): a list of events the plugin is interested in, anyone can emit the event (it's just a string)
        """
        name: str = plugin.name
        if name not in self._plugins.keys():
            MutyLogger.get_instance().debug(
                "registering plugin %s to receive internal events: %s", name, types
            )
            self._plugins[name] = {
                "plugin_instance": plugin,  # the plugin instance
                "types": (
                    types if types else []
                ),  # events the plugin may receive in its `internal_event_callback`
            }
        # else:
        #     MutyLogger.get_instance().warning(
        #         "plugin %s already registered to receive internal events" % (name)
        #     )

    def is_plugin_registered(self, plugin: str) -> bool:
        """
        check if a plugin is registered to receive internal events

        Args:
            plugin (str): the name of the plugin to check
        Returns:
            bool: True if the plugin is registered, False otherwise
        """
        return plugin in self._plugins.keys()

    def deregister(self, plugin: str) -> None:
        """
        Stop a plugin from receiving internal events.

        Args:
            plugin (str): The name of the plugin to unregister.
        """
        if plugin in self._plugins.keys():
            MutyLogger.get_instance().debug(
                "deregistering plugin %s from receiving internal events", plugin
            )
            del self._plugins[plugin]
        else:
            MutyLogger.get_instance().debug(
                "plugin %s not registered to receive internal events", plugin
            )

    async def dispatch_internal_event(
        self,
        t: str,
        data: dict = None,
        user_id: str = None,
        req_id: str = None,
        operation_id: str = None,
    ) -> dict:
        """
        dispatches internal event to all plugins which registered for it: each plugin's `internal_event_callback` is called with the event data, 
        and the callback return value is fed back to the next plugin as input data (i.e. plugins can modify the event data and pass it to the next plugin).

        NOTE: this can be used by the main process only, which holds the GulpInternalEventsManager singleton.

        in workers, plugins should call GulpRedisBroker.put_internal_event() or GulpRedisBroker.put_internal_wait() 
        to send internal events to the main process, which will then call this method to dispatch the event to the relevant plugins.

        Args:
            t: str: the event (must be previously registered with GulpInternalEventsManager.register)
            data (dict, optional): The data to send with the event (event-specific). Defaults to None.
            user_id (str, optional): the user id associated with this event. Defaults to None.
            req_id (str, optional): the request id associated with this event. Defaults to None.
            operation_id (str, optional): the operation id if applicable. Defaults to None.
        Returns:
            dict: the result of the event, including the plugins that handled it and the callback return value (if any)
        """
        ev: GulpInternalEvent = GulpInternalEvent(
            type=t,
            timestamp_msec=muty.time.now_msec(),
            data=data,
            user_id=user_id,
            operation_id=operation_id,
            req_id=req_id,
        )

        result= GulpInternalEventResult(plugins=[], event=t, result=data)
        for _, entry in self._plugins.items():
            p: GulpPluginBase = entry["plugin_instance"]
            if t in entry["types"]:
                # if this plugin manages event of t type ...
                try:
                    MutyLogger.get_instance().debug(
                        "dispatching internal event %s to plugin %s", t, p.name
                    )
                    res = await p.internal_event_callback(ev)
                    if res and res.result:
                        # feed the result back for the next plugin as input
                        ev.data = res.result
                        result.plugins.append(p.name)
                        if res.stop:
                            # stop here
                            break


                except Exception as e:
                    MutyLogger.get_instance().exception(e)

        return result.model_dump()



