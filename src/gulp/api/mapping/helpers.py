import json
import logging

import muty.file
from sigma.processing.conditions import LogsourceCondition
from sigma.processing.pipeline import ProcessingItem, ProcessingPipeline
from sigma.processing.transformations import FieldMappingTransformation

from gulp.api.mapping.models import GulpMappingField, GulpMapping
from gulp.utils import logger


async def _get_mappings_internal(mapping_file_path: str) -> dict:
    """Check for file existance and return json file as dict

    Args:
        mapping_file_path (str): file to load mappings from

    Raises:
        FileNotFoundError: file does not exist

    Returns:
        dict: the loaded dictionary
    """
    l = logger()
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


async def get_mappings_from_file(mapping_file_path: str) -> list[GulpMapping]:
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
    mappings = []

    l = logger()
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
    l = logger()
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



