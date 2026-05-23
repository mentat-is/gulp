from __future__ import annotations

import os
import json
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

import muty.crypto
from muty.log import MutyLogger
import orjson
import muty.time
from dateutil import parser as dateparser

from gulp.api.mapping.models import GulpMapping, GulpMappingFile
from gulp.api.opensearch.structs import GulpDocument
from gulp.config import GulpConfig
from gulp.structs import GulpMappingParameters

_MISSING = object()


def mapping_attr(field_mapping: Any, name: str, default: Any = None) -> Any:
    """Read one mapping attribute from either a dict or a model-like object."""

    if isinstance(field_mapping, dict):
        return field_mapping.get(name, default)
    return getattr(field_mapping, name, default)


def normalize_timestamp(value: Any, timestamp_format: str | None = None) -> str:
    """Normalize supported timestamp inputs into UTC ISO8601 with millisecond precision."""

    dt: datetime

    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        abs_v = abs(float(value))
        if abs_v >= 1e18:
            dt = datetime.fromtimestamp(float(value) / 1e9, tz=timezone.utc)
        elif abs_v >= 1e15:
            dt = datetime.fromtimestamp(float(value) / 1e6, tz=timezone.utc)
        elif abs_v >= 1e12:
            dt = datetime.fromtimestamp(float(value) / 1e3, tz=timezone.utc)
        else:
            dt = datetime.fromtimestamp(float(value), tz=timezone.utc)
    elif isinstance(value, str):
        if timestamp_format:
            dt = datetime.strptime(value, timestamp_format)
        else:
            dt = dateparser.parse(value)
    else:
        raise ValueError(f"Unsupported timestamp value type: {type(value)!r}")

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.isoformat(timespec="milliseconds").replace("+00:00", "Z")


def flatten_json_value(value: Any, prefix: str = "") -> dict[str, Any]:
    """Flatten a JSON object into dotted keys, optionally under one field prefix."""

    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise ValueError("flatten_json requires a JSON object value")

    flattened: dict[str, Any] = {}

    def _walk(current_prefix: str, current: Any) -> None:
        if isinstance(current, dict):
            for key, child in current.items():
                next_prefix = f"{current_prefix}.{key}" if current_prefix else str(key)
                _walk(next_prefix, child)
            return
        flattened[current_prefix] = current

    _walk(prefix, value)
    return flattened


def transform_scalar(
    value: Any,
    *,
    force_type: str | None = None,
    multiplier: float | int | None = None,
    multiplier_first: bool = True,
    invalid_int: Any = _MISSING,
    invalid_float: Any = _MISSING,
) -> Any:
    """Apply simple multiplier/type coercion rules shared by gulp mapping flows."""

    transformed = value

    if multiplier_first and multiplier is not None and transformed is not None:
        transformed = float(transformed) * float(multiplier)

    if force_type == "int":
        try:
            transformed = int(transformed)
        except (TypeError, ValueError):
            if invalid_int is _MISSING:
                raise
            transformed = invalid_int
    elif force_type == "float":
        try:
            transformed = float(transformed)
        except (TypeError, ValueError):
            if invalid_float is _MISSING:
                raise
            transformed = invalid_float
    elif force_type == "str":
        transformed = str(transformed)

    if not multiplier_first and multiplier is not None and transformed is not None:
        transformed = float(transformed) * float(multiplier)

    return transformed


def convert_special_timestamp(
    value: Any,
    *,
    timestamp_kind: str | None = None,
    timestamp_format: str | None = None,
    output: str = "iso8601",
) -> Any:
    """Convert mapping-specific timestamp encodings into the requested output format."""

    kind = timestamp_kind or "generic"

    if output == "iso8601":
        if kind == "windows_filetime":
            base = datetime(1601, 1, 1, tzinfo=timezone.utc)
            dt = base + timedelta(microseconds=int(value) / 10)
            return normalize_timestamp(dt)
        if kind == "chrome":
            base = datetime(1601, 1, 1, tzinfo=timezone.utc)
            dt = base + timedelta(microseconds=float(value))
            return normalize_timestamp(dt)
        if kind == "generic":
            return normalize_timestamp(value, timestamp_format)
        raise ValueError(f"Unsupported timestamp kind for ISO8601 output: {kind}")

    if output == "unix_nanos":
        if kind == "chrome":
            return muty.time.chrome_epoch_to_nanos_from_unix_epoch(int(value))
        if kind == "windows_filetime":
            return muty.time.windows_filetime_to_nanos_from_unix_epoch(int(value))
        if kind == "generic":
            _, ns, _ = GulpDocument.ensure_timestamp(
                str(value), format_string=timestamp_format
            )
            return ns
        raise ValueError(f"Unsupported timestamp kind for unix_nanos output: {kind}")

    raise ValueError(f"Unsupported timestamp output format: {output}")


def apply_value_aliases(
    mapped_key: str,
    d: dict,
    value_aliases: dict[str, dict[str, dict]],
    record_type: str | None = None,
) -> dict:
    """Apply value alias remapping for one mapped key and record type.

    Alias resolution order matches plugin behavior:
    1) pick aliases for record_type when available
    2) otherwise fallback to the "default" alias map
    """

    if not value_aliases or mapped_key not in value_aliases:
        return d

    alias_bucket = value_aliases[mapped_key]
    selected_record_type = record_type or "default"

    if selected_record_type in alias_bucket:
        alias_map = alias_bucket[selected_record_type]
    else:
        alias_map = alias_bucket.get("default", {})

    if not alias_map:
        return d

    for key, value in d.items():
        alias_key = str(value)
        if alias_key in alias_map:
            d[key] = alias_map[alias_key]

    return d


def build_extra_doc_fields(
    field_mapping: Any,
    *,
    timestamp_value: Any,
    mapped_fields: Iterable[str] = (),
    event_code_field: str = "event_code",
    timestamp_field: str = "timestamp",
) -> dict[str, Any]:
    """Build the per-document override payload for one extra mapped document.

    The returned dictionary contains the extra document's event code and
    timestamp override plus `None` placeholders for any fields that should be
    cleared from the derived document.
    """

    event_code = mapping_attr(field_mapping, "extra_doc_with_event_code")
    if not event_code:
        raise ValueError("extra_doc_with_event_code is required to build extra docs")

    extra_doc = {
        event_code_field: str(event_code),
        timestamp_field: timestamp_value,
    }
    for field_name in mapped_fields:
        extra_doc[field_name] = None
    return extra_doc


def expand_extra_docs(
    base_doc: dict[str, Any],
    extra_docs: list[dict[str, Any]],
    *,
    base_document_id: str | None = None,
) -> list[dict[str, Any]]:
    """Expand a base mapped document into the final list of sibling documents."""

    if not extra_docs:
        return [base_doc]

    docs = [base_doc]
    for extra_fields in extra_docs:
        new_doc = {**base_doc, **extra_fields}
        if base_document_id:
            new_doc["gulp.base_document_id"] = base_document_id
        docs.append(new_doc)
    return docs


def build_gulp_document_id(
    *,
    event_original: str,
    event_code: str,
    operation_id: str,
    context_id: str,
    source_id: str,
    event_sequence: int,
    timestamp: str,
) -> str:
    """Compute the deterministic gULP document ID using the core hash contract."""

    _, ts_nanos, _ = GulpDocument.ensure_timestamp(str(timestamp))
    return muty.crypto.hash_xxh128(
        f"{event_original}{event_code}{operation_id}{context_id}{source_id}{event_sequence}{ts_nanos}"
    )


async def mapping_parameters_to_mapping(
    mapping_parameters: GulpMappingParameters = None, mapping_base_path: str = None
) -> tuple[dict[str, GulpMapping], str]:
    """
    get each defined mapping, handling loading from file if needed, and merging additional mappings if specified.

    Args:
        mapping_parameters (GulpMappingParameters, optional): the mapping parameters. if not set, the default (empty) mapping will be used.
        mapping_base_path (str, optional): the base path to resolve relative mapping file paths against. if not set, relative paths will be resolved against the current working directory.
    Returns:
        tuple[dict[str, GulpMapping], str]: a tuple with the mappings (if empty, this is set to an empty mapping with mapping_id="default") and the mapping id
    """

    def _check_abs_path(filename: str, base_path: str = None) -> tuple[str, bool]:
        """
        check if an absolute path is provided and exists
        """
        if base_path:
            # join the path with the base path if provided, this allows us to support both absolute and relative paths in mapping_parameters
            filename = os.path.join(base_path, filename)

        if os.path.isabs(filename):
            if os.path.exists(filename):
                # absolute path provided
                return filename, True
            # invalid path
            return None, False

        # not an absolute path
        return filename, False

    if not mapping_parameters:
        mapping_parameters = GulpMappingParameters()

    mappings: dict[str, GulpMapping] = None
    mapping_id: str = None
    if (
        not mapping_parameters.mapping_file
        and not mapping_parameters.mappings
        and not mapping_parameters.additional_mapping_files
        and not mapping_parameters.additional_mappings
        and mapping_parameters.mapping_id
    ):
        raise ValueError(
            "mapping_id is set but mappings/mapping_file/additional_mapping_files/additional_mappings are not!"
        )

    # check if mappings or mapping_file is set
    if mapping_parameters.mappings:
        # use provided mappings dictionary
        mappings = {
            k: GulpMapping.model_validate(v)
            for k, v in mapping_parameters.mappings.items()
        }
        MutyLogger.get_instance().debug(
            f'using plugin_params.mapping_parameters.mappings="{mapping_parameters.mappings}"'
        )
    elif mapping_parameters.mapping_file:
        # load from mapping file, check if its an absolute path first
        mapping_file = mapping_parameters.mapping_file
        # MutyLogger.get_instance().debug(
        #     f"using plugin_params.mapping_parameters.mapping_file={mapping_file}"
        # )
        mapping_file_path, is_absolute_path = _check_abs_path(
            mapping_file, mapping_base_path
        )
        if not mapping_file_path:
            raise FileNotFoundError(f"mapping file {mapping_file} does not exist!")
        if not is_absolute_path:
            mapping_file_path = GulpConfig.get_instance().build_mapping_file_path(
                mapping_file
            )

        file_content = await muty.file.read_file_async(mapping_file_path)
        mapping_data = orjson.loads(file_content)

        if not mapping_data:
            raise ValueError(f"mapping file {mapping_file_path} is empty!")

        mapping_file_obj = GulpMappingFile.model_validate(mapping_data)
        mappings = mapping_file_obj.mappings

    # validation checks
    if not mappings and not mapping_parameters.mapping_id:
        # empty mapping will be used
        MutyLogger.get_instance().warning(
            "mappings/mapping_file and mapping_id are both None/empty!"
        )
        mappings = {"default": GulpMapping(fields={})}

    # ensure mapping_id is set to first key if not specified
    mapping_id = mapping_parameters.mapping_id or list(mappings.keys())[0]
    # MutyLogger.get_instance().debug(f"mapping_id={mapping_id}")

    # if we have specified direct mapping alone, just stop here and use it
    if mapping_parameters.mappings and (
        not mapping_parameters.additional_mapping_files
        and not mapping_parameters.additional_mappings
    ):
        return mappings, mapping_id

    # we may have additional mapping specified in mapping_parameters.additional_mapping_files and/or
    # mapping_parameters.additional_mappings. so, merge them
    if mapping_parameters.additional_mapping_files:
        MutyLogger.get_instance().debug(
            f"loading additional mapping files/id: {mapping_parameters.additional_mapping_files} ..."
        )

        for file_info in mapping_parameters.additional_mapping_files:
            # load and merge additional mappings from files, check for absolute paths first
            additional_file_path, is_absolute_path = _check_abs_path(
                file_info[0], mapping_base_path
            )
            if not additional_file_path:
                MutyLogger.get_instance().error(
                    f"mapping file {file_info[0]} does not exist!"
                )
                continue
            if not is_absolute_path:
                additional_file_path = (
                    GulpConfig.get_instance().build_mapping_file_path(file_info[0])
                )

            additional_mapping_id = file_info[1]

            file_content = await muty.file.read_file_async(additional_file_path)
            mapping_data = orjson.loads(file_content)

            if not mapping_data:
                raise ValueError(
                    f"additional mapping file {additional_file_path} is empty!"
                )

            additional_mapping_file = GulpMappingFile.model_validate(mapping_data)

            # merge mappings
            main_mapping = mappings.get(mapping_id, GulpMapping())
            add_mapping = additional_mapping_file.mappings[additional_mapping_id]

            MutyLogger.get_instance().debug(
                f"adding additional mappings from {additional_file_path}.{additional_mapping_id} to '{mapping_id}' ..."
            )

            for key, value in add_mapping.fields.items():
                main_mapping.fields[key] = value

            mappings[mapping_id] = main_mapping

    if mapping_parameters.additional_mappings:
        MutyLogger.get_instance().debug(
            f"loading additional mappings: {mapping_parameters.additional_mappings} ..."
        )

        for (
            additional_mapping_id,
            additional_mapping,
        ) in mapping_parameters.additional_mappings.items():
            # merge additional mappings
            main_mapping = mappings.get(mapping_id, GulpMapping())
            add_mapping = GulpMapping.model_validate(additional_mapping)

            MutyLogger.get_instance().debug(
                f"adding additional mappings from {additional_mapping_id} to '{mapping_id}' ..."
            )
            for key, value in add_mapping.fields.items():
                main_mapping.fields[key] = value

            mappings[mapping_id] = main_mapping

    # MutyLogger.get_instance().debug(f"************ final mappings for mapping_id {mapping_id}:\n{mappings}")
    return mappings, mapping_id
