import json
import logging

import muty.file
from sigma.processing.conditions import LogsourceCondition
from sigma.processing.pipeline import ProcessingItem, ProcessingPipeline
from sigma.processing.transformations import FieldMappingTransformation

from gulp.api.mapping.models import FieldMappingEntry, GulpMapping

_logger: logging.Logger = None


def init(logger: logging.Logger):
    """
    Initializes the helper module

    Args:
        logger (logging.Logger): The logger instance.

    """
    global _logger
    _logger = logger

async def _get_mappings_internal(mapping_file_path: str) -> dict:
    """Check for file existance and return json file as dict

    Args:
        mapping_file_path (str): file to load mappings from

    Raises:
        FileNotFoundError: file does not exist

    Returns:
        dict: the loaded dictionary
    """
    l = _logger
    if l is None:
        l = logging.getLogger()

    # read mappings from file path, if it exists
    exists = await muty.file.exists_async(mapping_file_path)
    if not exists:
        l.warning("mapping file not found: %s" % (mapping_file_path))
        raise FileNotFoundError("mapping file not found: %s" % (mapping_file_path))

    # load the mapping file
    l.debug("loading mapping file: %s" % (mapping_file_path))
    buf = await muty.file.read_file_async(mapping_file_path)
    js = json.loads(buf)
    mappings = js["mappings"]
    return mappings

async def get_mappings_from_file(mapping_file_path:str) -> list[GulpMapping]:
    """
        Retrieve all mappings from a file.

    Args:
        mapping_file_path (str): The path to the mapping file.

    Returns:
        list[GulpMapping]: the mappings

    Raises:
        FileNotFoundError: if the mapping file does not exist
        ValueError: if no mapping_id are found
    """
    mappings=[]

    l = _logger
    if l is None:
        l = logging.getLogger()

    maps = await _get_mappings_internal(mapping_file_path)

    for mapping in maps:
        mappings.append(GulpMapping.from_dict(mapping))

    return mappings

async def get_mapping_from_file(
    mapping_file_path: str, mapping_id: str = None
) -> GulpMapping:
    """
        Retrieve the mapping from a file.

    Args:
        mapping_file_path (str): The path to the mapping file.
        mapping_id (str): The mapping id to retrieve from the file (default: None, return first mapping only).

    Returns:
        GulpMapping: the mapping

    Raises:
        FileNotFoundError: if the mapping file does not exist
        ValueError: if the mapping_id is not found in the mapping file

    NOTE: the following is an example of the mapping file format to indicate different mapping options and styles (single, multiple, variable mapping)

    ~~~json
        {
    // an array of mapppings and related options
    "mappings": [{
            "fields": {
                // source field
                "EntryNumber": {
                    // map to single string
                    "map_to": "event.sequence"
                }
            },
            "options": {
                // set "event.code" to "record" if this mapping is used
                "event_code": "record",
                // set "agent.type" to "mftecmd" if this mapping is used
                "agent_type": "mftecmd",
                // this is to identify this mapping in the whole file
                "mapping_id": "record"
            }
        },
        {
            "fields": {
                "SourceFile": {
                    // multiple mapping
                    // SourceFile will be mapped to both "file.path" and "file.name"
                    "map_to": ["file.path", "file.name"]
                }
            },
            "options": {
                "mapping_id": "boot"
            }
        },
        {
            "fields": {
                "Name": {
                    // variable mapping
                    // Name will be mapped to "user.name" and "service.name"
                    "map_to": [
                        ["service", "security", "user.name"],
                        ["service", "security", "service.name"]
                    ],
                    "is_variable_mapping": true
                }
            },
            "options": {
                "mapping_id": "j"
            }
        }
    ]}
    ~~~
    """
    l = _logger
    if l is None:
        l = logging.getLogger()

    mappings = await _get_mappings_internal(mapping_file_path)
    if mapping_id is None:
        l.warning("no mapping_id set, returning first element: %s" % (mappings[0]))
        m = GulpMapping.from_dict(mappings[0])
        return m

    # get specific mapping
    for m in mappings:
        options = m.get("options", None)
        if options is not None:
            if options.get("mapping_id", None) == mapping_id:
                l.debug("mapping found for mapping_id=%s: %s" % (mapping_id, m))
                return GulpMapping.from_dict(m)
    raise ValueError("mapping_id not found in the mapping file: %s" % (mapping_id))


async def get_enriched_mapping_for_ingestion(
    pipeline: ProcessingPipeline = None,
    mapping_file_path: str = None,
    mapping_id: str = None,
    product: str = None,
) -> GulpMapping:
    """
    Retrieves an enriched mapping by merging a pipeline mapping and a file mapping.

    NOTE: This is to be used solely by the INGESTION plugins.

    Args:
        pipeline (ProcessingPipeline, optional): The processing pipeline containing the mapping. Defaults to None.
        mapping_file_path (str, optional): The file path of the mapping file. Defaults to None.
        mapping_id (str, optional): The ID of the mapping. Defaults to None.

    Returns:
        GulpMapping: The enriched mapping, may be an empty GulpMapping if i.e. both pipeline and mapping_file_path are not provided.

    """
    l = _logger
    if l is None:
        l = logging.getLogger()

    pipeline_mapping: GulpMapping = None
    file_mapping: GulpMapping = None

    if pipeline is None and mapping_file_path is None:
        # return an empty mapping
        return GulpMapping.from_dict({})

    if mapping_file_path is not None:
        # get mapping from file
        try:
            file_mapping = await get_mapping_from_file(
                mapping_file_path, mapping_id=mapping_id
            )
        except Exception as ex:
            l.exception(
                "error loading mapping file: %s, ex=%s" % (mapping_file_path, ex)
            )
            if pipeline is None:
                return GulpMapping.from_dict({})

    if pipeline is not None:
        # get mapping from pipeline, convert each FieldMappingTransformation item to GulpMapping
        l.debug("turning provided pipeline to GulpMapping ...")
        d: dict = {
            "fields": {},
            "options": (
                file_mapping.options.to_dict()
                if file_mapping is not None and file_mapping.options is not None
                else None
            ),
        }
        for item in pipeline.items:
            if isinstance(item.transformation, FieldMappingTransformation):
                for k, v in item.transformation.mapping.items():
                    dd = {"map_to": v}
                    d["fields"][k] = dd

        pipeline_mapping = GulpMapping.from_dict(d)

    if pipeline_mapping is None:
        # return mapping from file
        l.warning(
            "no pipeline provided, returning file mapping: %s"
            % (json.dumps(file_mapping.to_dict(), indent=2))
        )

        return file_mapping

    if file_mapping is None:
        l.warning(
            "no file mapping provided, returning pipeline mapping: %s"
            % (json.dumps(pipeline_mapping.to_dict(), indent=2))
        )
        return pipeline_mapping

    # merge mapping into pipeline_mapping
    l.debug("merging file mapping into pipeline mapping ...")
    # l.debug("pipeline_mapping PRE=\n%s" % (json.dumps(pipeline_mapping.to_dict(), indent=2)))
    # l.debug("file_mapping=%s" % (json.dumps(file_mapping.to_dict(), indent=2)))
    for m, v in file_mapping.fields.items():
        if m not in pipeline_mapping.fields.keys():
            # this seems a pylint issue: https://github.com/pylint-dev/pylint/issues/2767 and related
            # pylint: disable=unsupported-assignment-operation
            pipeline_mapping.fields[m] = v
        else:
            # merge
            pipeline_v: FieldMappingEntry = pipeline_mapping[m]
            file_v: FieldMappingEntry = v
            if file_v.is_variable_mapping:
                # "map_to" is a list[list[str]]
                # where each member of the inner list is a variable mapping with 3 strings(logsourcename, logsource, mapped)
                # since we're calling this for ingestion only, we simply convert the affected "map_to" to a multiple string mapping, to map the field to multiple values
                real_map_to = []
                for vm in file_v.map_to:
                    # logsrc_field = vm[0]
                    # logsrc = vm[1]
                    mapped = vm[2]
                    real_map_to.append(mapped)
                file_v.map_to = real_map_to

            # depending if the source (file) and destination (pipeline) mapping are strings or lists, we need to merge them accordingly
            if file_v.map_to is not None:
                if isinstance(pipeline_v.map_to, list) and isinstance(file_v.map_to, list):
                    pipeline_v.map_to.extend(file_v.map_to)
                elif isinstance(pipeline_v.map_to, str) and isinstance(file_v.map_to, str):
                    pipeline_v.map_to = [pipeline_v.map_to, file_v.map_to]
                elif isinstance(pipeline_v.map_to, list) and isinstance(file_v.map_to, str):
                    pipeline_v.map_to.append(file_v.map_to)
                elif isinstance(pipeline_v.map_to, str) and isinstance(file_v.map_to, list):
                    file_v.map_to.append(pipeline_v.map_to)
                    pipeline_v.map_to = file_v.map_to

            # set other options from the file mapping
            pipeline_v.is_timestamp = file_v.is_timestamp
            pipeline_v.event_code = file_v.event_code

    # l.debug("MERGED mappings: %s" % (json.dumps(pipeline_mapping.to_dict(), indent=2)))
    merged_mapping = pipeline_mapping
    return merged_mapping


async def get_enriched_pipeline(
    pipeline: ProcessingPipeline = None,
    mapping_file_path: str = None,
    mapping_id: str = None,
    product: str = None,
    **kwargs,
) -> ProcessingPipeline:
    """
    Returns an enriched pysigma processing pipeline (base ProcessingPipeline mapping + file mapping) to be used to convert SIGMA RULES to ELASTICSEARCH DSL QUERY.

    NOTE: This is to be used solely by the SIGMA plugins.

    Args:
        pipeline (ProcessingPipeline): optional, the base processing pipeline to enrich (default: None, empty pipeline)
        mapping_file_path (str): optional, the path to the mapping file to load mappings from (default: None)
        mapping_id (str): optional, the mapping id to retrieve from the mapping file (default: None, first mapping only)
        product (str): optional, the product name to set in the resulting pipeline LogSourceCondition array (default: None, inferred from the file name if mapping_file_path is provided: /path/to/product.json -> product)
        kwargs: additional keyword arguments
    Returns:
        ProcessingPipeline: The enriched processing pipeline.
        if no mapping_file_path, the original pipeline (or an empty pipeline if base is None) is returned.
    """
    l = _logger
    if l is None:
        l = logging.getLogger()

    if pipeline is None:
        # use default pipeline as base
        l.debug("no pipeline provided, using empty pipeline.")
        pipeline = ProcessingPipeline()

    if mapping_file_path is None:
        # no file mapping provided, return the original pipeline
        l.debug("no file mapping provided, using just the provided pipeline.")
        return pipeline

    try:
        mapping = await get_mapping_from_file(mapping_file_path, mapping_id=mapping_id)
        product = mapping_file_path.split("/")[-1].split(".")[0]
    except:
        l.exception("error loading mapping file: %s" % (mapping_file_path))
        return pipeline

    # enrich pipeline

    # collect standard (single or multiple string) and variable mapping from FILE
    std_mapping = {}
    var_mapping = {}
    for k, v in mapping.fields.items():
        vv: FieldMappingEntry = v
        if isinstance(vv.map_to, str):
            # single, map k to vv.map_to
            std_mapping[k] = [vv.map_to]
        elif isinstance(vv.map_to, list):
            if vv.is_variable_mapping:
                # variable mapping, map k to vv.map_to which is a list of lists [logsource_field_name, logsource, mapped_field]
                var_mapping[k] = vv.map_to
            else:
                # multiple, map k to vv.map_to which is a list of string
                std_mapping[k] = vv.map_to

    p_items: list[ProcessingItem] = []

    # create processing items for each std_mapping
    if len(std_mapping) > 0:
        # use product only for rule conditions
        rule_conditions: list[LogsourceCondition] = []
        if product is not None:
            rule_conditions = [LogsourceCondition(product=product)]

        for k, v in std_mapping.items():
            p = ProcessingItem(
                identifier="gulp-field_mapping",
                transformation=FieldMappingTransformation(std_mapping),
                rule_conditions=rule_conditions,
            )
            p_items.append(p)

    # create processing items for each variable mapping
    if len(var_mapping) > 0:
        # we will use the both product and logsource field/name for rule conditions
        for k, v in var_mapping.items():
            for m in v:
                logsrc_field = m[0]
                logsrc = m[1]
                mapped = m[2]
                p = ProcessingItem(
                    identifier="gulp-variable_field_mapping-%s-%s-%s"
                    % (k, logsrc_field, logsrc),
                    transformation=FieldMappingTransformation({k: mapped}),
                    rule_conditions=[
                        LogsourceCondition(
                            **{
                                "product": product,
                                logsrc_field: logsrc,
                            }
                        ),
                    ],
                )
                p_items.append(p)

    # return the extended pipeline
    pipeline.items.extend(p_items)
    return pipeline
