"""Win PE file processor with ECS-oriented field naming."""

import asyncio
import datetime
import hashlib
import math
import os
from pathlib import Path
from typing import Any, override

import muty.os
import orjson
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.collab.stats import GulpRequestStats
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

muty.os.check_and_install_package("pefile", ">=2024.8.26")

import pefile
import peutils


class Plugin(GulpPluginBase):
    _PE_MIME_TYPE = "application/vnd.microsoft.portable-executable"
    _ARCHITECTURE_MAP = {
        "IMAGE_FILE_MACHINE_I386": "x86",
        "IMAGE_FILE_MACHINE_AMD64": "x64",
        "IMAGE_FILE_MACHINE_ARM": "arm",
        "IMAGE_FILE_MACHINE_ARMNT": "arm",
        "IMAGE_FILE_MACHINE_ARM64": "arm64",
        "IMAGE_FILE_MACHINE_IA64": "ia64",
    }

    @override
    def desc(self) -> str:
        return """generic PE file processor.

NOTE: this plugin may not work reliably since gulp needs a reliable timestamp for the documents it ingests, and win10+ changes in the PE file format doesn't provide a reliable timestamp in the PE header.
for reference, read this blog post: https://devblogs.microsoft.com/oldnewthing/20180103-00/?p=97705    
"""

    def display_name(self) -> str:
        return "win_pe"

    def regex(self) -> str:
        """regex to identify this format"""
        return "^\x4d\x5a"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="include_relocations",
                type="bool",
                desc="include base relocations information",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="entropy_checks",
                type="bool",
                desc="include entropy checks (is_suspicious, is_probably_packed)",
                default_value=True,
            ),
            GulpPluginCustomParameter(
                name="keep_warnings",
                type="bool",
                desc="do not discard pefile parsing warnings from document",
                default_value=False,
            ),
        ]

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    @staticmethod
    def _decode_bytes(value: bytes) -> str:
        return value.decode("utf-8", errors="replace")

    @classmethod
    def _sanitize_value(cls, value: Any) -> Any:
        if isinstance(value, bytes):
            return cls._decode_bytes(value)
        if isinstance(value, dict):
            return {str(k): cls._sanitize_value(v) for k, v in value.items()}
        if isinstance(value, list):
            return [cls._sanitize_value(item) for item in value]
        if isinstance(value, tuple):
            return [cls._sanitize_value(item) for item in value]
        return value

    @staticmethod
    def _parse_dump_value(value: Any) -> Any:
        if isinstance(value, dict) and "Value" in value:
            value = value["Value"]
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("0x"):
                token = stripped.split()[0]
                try:
                    return int(token, 16)
                except ValueError:
                    return value
            if stripped.isdigit():
                try:
                    return int(stripped)
                except ValueError:
                    return value
        return value

    @classmethod
    def _parse_dump_string(cls, value: Any) -> str | None:
        parsed = cls._parse_dump_value(value)
        if parsed is None:
            return None
        if isinstance(parsed, bytes):
            return cls._decode_bytes(parsed)
        return str(parsed)

    @classmethod
    def _machine_to_architecture(cls, machine_value: Any) -> str | None:
        machine_string = cls._parse_dump_string(machine_value)
        if not machine_string:
            return None

        for token, architecture in cls._ARCHITECTURE_MAP.items():
            if token in machine_string:
                return architecture

        return machine_string

    @staticmethod
    def _safe_getattr(value: Any, attr: str, default: Any = None) -> Any:
        return getattr(value, attr, default) if value is not None else default

    @staticmethod
    def _compute_entropy(values: list[str]) -> float | None:
        payload = "".join(value for value in values if value)
        if not payload:
            return None

        counts: dict[str, int] = {}
        for char in payload:
            counts[char] = counts.get(char, 0) + 1

        total = len(payload)
        entropy = 0.0
        for count in counts.values():
            probability = count / total
            entropy -= probability * math.log2(probability)

        return entropy

    @staticmethod
    def _hash_file_sync(path: str) -> dict[str, str]:
        md5 = hashlib.md5()
        sha1 = hashlib.sha1()
        sha256 = hashlib.sha256()

        with open(path, "rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                md5.update(chunk)
                sha1.update(chunk)
                sha256.update(chunk)

        return {
            "hash.md5": md5.hexdigest(),
            "hash.sha1": sha1.hexdigest(),
            "hash.sha256": sha256.hexdigest(),
        }

    @classmethod
    async def _hash_file(cls, path: str) -> dict[str, str]:
        return await asyncio.to_thread(cls._hash_file_sync, path)

    @classmethod
    def _extract_version_info(cls, raw_dump: dict[str, Any]) -> dict[str, str]:
        version_info = raw_dump.get("Version Information", [])
        extracted: dict[str, str] = {}
        wanted = {
            "CompanyName": "file.pe.company",
            "FileDescription": "file.pe.description",
            "FileVersion": "file.pe.file_version",
            "OriginalFilename": "file.pe.original_file_name",
            "ProductName": "file.pe.product",
        }

        def _walk(node: Any) -> None:
            if isinstance(node, dict):
                for key, value in node.items():
                    if key in wanted and value not in (None, ""):
                        extracted[wanted[key]] = cls._parse_dump_string(value)
                    else:
                        _walk(value)
            elif isinstance(node, list):
                for item in node:
                    _walk(item)

        _walk(version_info)
        return extracted

    @classmethod
    def _extract_sections(cls, raw_dump: dict[str, Any]) -> list[dict[str, Any]]:
        sections: list[dict[str, Any]] = []
        for section in raw_dump.get("PE Sections", []):
            name = cls._parse_dump_string(section.get("Name"))
            physical_size = cls._parse_dump_value(section.get("SizeOfRawData"))
            virtual_size = cls._parse_dump_value(
                section.get("Misc_VirtualSize", section.get("VirtualSize"))
            )
            entropy = section.get("Entropy")

            normalized: dict[str, Any] = {}
            if name:
                normalized["name"] = name.rstrip("\x00")
            if isinstance(physical_size, int):
                normalized["physical_size"] = physical_size
            if isinstance(virtual_size, int):
                normalized["virtual_size"] = virtual_size
            if isinstance(entropy, (int, float)):
                normalized["entropy"] = int(entropy)

            if normalized:
                sections.append(normalized)

        return sections

    @classmethod
    def _extract_imports(cls, record: pefile.PE) -> list[str]:
        imports: list[str] = []
        for descriptor in getattr(record, "DIRECTORY_ENTRY_IMPORT", []) or []:
            library_name = cls._sanitize_value(getattr(descriptor, "dll", None))
            for imported_symbol in getattr(descriptor, "imports", []) or []:
                symbol_name = cls._sanitize_value(
                    getattr(imported_symbol, "name", None)
                )
                ordinal = getattr(imported_symbol, "ordinal", None)
                if symbol_name and library_name:
                    imports.append(f"{library_name}!{symbol_name}")
                elif symbol_name:
                    imports.append(str(symbol_name))
                elif ordinal is not None and library_name:
                    imports.append(f"{library_name}!#{ordinal}")
                elif library_name:
                    imports.append(str(library_name))
        return imports

    @staticmethod
    def _imports_to_ecs_object(imports: list[str]) -> dict[str, Any]:
        # The active template maps file.pe.imports as object.
        # Keep import entries under a stable child key.
        return {"values": imports}

    @classmethod
    def _extract_exports(cls, record: pefile.PE) -> list[str]:
        exports: list[str] = []
        directory = getattr(record, "DIRECTORY_ENTRY_EXPORT", None)
        if not directory:
            return exports

        for exported_symbol in getattr(directory, "symbols", []) or []:
            name = cls._sanitize_value(getattr(exported_symbol, "name", None))
            ordinal = getattr(exported_symbol, "ordinal", None)
            if name:
                exports.append(str(name))
            elif ordinal is not None:
                exports.append(f"#{ordinal}")
        return exports

    @classmethod
    def _extract_relocations(cls, record: pefile.PE) -> list[dict[str, Any]]:
        relocations: list[dict[str, Any]] = []
        for relocation_block in getattr(record, "DIRECTORY_ENTRY_BASERELOC", []) or []:
            block = {
                "virtual_address": getattr(
                    relocation_block.struct, "VirtualAddress", None
                ),
                "size_of_block": getattr(relocation_block.struct, "SizeOfBlock", None),
                "entries": [],
            }
            for entry in getattr(relocation_block, "entries", []) or []:
                block["entries"].append(
                    {
                        "rva": getattr(entry, "rva", None),
                        "base_rva": getattr(entry, "base_rva", None),
                        "type": getattr(entry, "type", None),
                    }
                )
            relocations.append(block)
        return relocations

    @classmethod
    def _get_compile_timestamp(cls, record: pefile.PE) -> int:
        timestamp = cls._safe_getattr(
            cls._safe_getattr(record, "FILE_HEADER"), "TimeDateStamp"
        )
        if not timestamp:
            return 0

        return int(timestamp)

    @staticmethod
    def _to_iso8601_utc(timestamp: int) -> str:
        if not timestamp:
            return "1970-01-01T00:00:00Z"
        return (
            datetime.datetime.fromtimestamp(timestamp, tz=datetime.timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z")
        )

    @staticmethod
    def _classify_pe(record: pefile.PE) -> str:
        if record.is_dll():
            return "dll"
        if record.is_driver():
            return "driver"
        if record.is_exe():
            return "exe"
        return "pe"

    async def _build_document_fields(
        self,
        record: pefile.PE,
        include_relocations: bool,
        entropy_checks: bool,
        keep_warnings: bool,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        raw_dump = self._sanitize_value(record.dump_dict())

        if not include_relocations:
            raw_dump.pop("Base relocations", None)
        if not keep_warnings:
            raw_dump.pop("Parsing Warnings", None)

        file_path = self._original_file_path or self._file_path
        path_obj = Path(file_path)
        hashes = await self._hash_file(self._file_path)

        fields: dict[str, Any] = {
            "event.category": ["file"],
            "event.type": ["info"],
            "event.action": "pe_metadata_extracted",
            "event.dataset": "win_pe",
            "file.path": file_path,
            "file.name": path_obj.name,
            "file.extension": path_obj.suffix.lstrip(".").lower(),
            "file.directory": (
                str(path_obj.parent) if str(path_obj.parent) not in ("", ".") else None
            ),
            "file.size": os.path.getsize(self._file_path),
            "file.type": "file",
            "file.mime_type": self._PE_MIME_TYPE,
            **hashes,
        }

        compile_timestamp = self._get_compile_timestamp(record)
        fields["file.created"] = self._to_iso8601_utc(compile_timestamp)

        architecture = self._machine_to_architecture(
            self._safe_getattr(self._safe_getattr(record, "FILE_HEADER"), "Machine")
            or raw_dump.get("FILE_HEADER", {}).get("Machine")
        )
        if architecture:
            fields["file.pe.architecture"] = architecture

        imphash = None
        try:
            imphash = record.get_imphash()
        except Exception:
            imphash = None
        if imphash:
            fields["file.pe.imphash"] = imphash
            fields["file.pe.import_hash"] = imphash

        for key, value in self._extract_version_info(raw_dump).items():
            if value:
                fields[key] = value

        sections = self._extract_sections(raw_dump)
        if sections:
            fields["file.pe.sections"] = sections

        imports = self._extract_imports(record)
        if imports:
            fields["file.pe.imports"] = self._imports_to_ecs_object(imports)
            imports_entropy = self._compute_entropy(imports)
            if imports_entropy is not None:
                fields["file.pe.imports_names_entropy"] = int(imports_entropy)

        exports = self._extract_exports(record)
        if exports:
            fields["gulp.win_pe.exports"] = exports

        if include_relocations:
            relocations = self._extract_relocations(record)
            if relocations:
                fields["gulp.win_pe.base_relocations"] = relocations

        if entropy_checks:
            fields["gulp.win_pe.analysis"] = {
                "is_probably_packed": peutils.is_probably_packed(record),
                "is_suspicious": peutils.is_suspicious(record),
                "is_valid": peutils.is_valid(record),
            }

        warnings = record.get_warnings()
        if keep_warnings and warnings:
            fields["gulp.win_pe.warnings"] = self._sanitize_value(warnings)

        return {
            key: value
            for key, value in fields.items()
            if value not in (None, "", [], {})
        }, raw_dump

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:
        include_relocations: bool = kwargs.get("include_relocations", False)
        entropy_checks: bool = kwargs.get("entropy_checks", True)
        keep_warnings: bool = kwargs.get("keep_warnings", False)

        fields, raw_dump = await self._build_document_fields(
            record,
            include_relocations=include_relocations,
            entropy_checks=entropy_checks,
            keep_warnings=keep_warnings,
        )
        event_code = self._classify_pe(record)
        timestamp = self._get_compile_timestamp(record)
        event_original = orjson.dumps(raw_dump).decode("utf-8")

        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_code=event_code,
            timestamp=timestamp,
            event_original=event_original,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **fields,
        )

    @override
    async def ingest_file(
        self,
        sess: AsyncSession,
        stats: GulpRequestStats,
        user_id: str,
        req_id: str,
        ws_id: str,
        index: str,
        operation_id: str,
        context_id: str,
        source_id: str,
        file_path: str,
        original_file_path: str = None,
        flt: GulpIngestionFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> GulpRequestStatus:
        plugin_params = self._ensure_plugin_params(plugin_params)
        await super().ingest_file(
            sess=sess,
            stats=stats,
            user_id=user_id,
            req_id=req_id,
            ws_id=ws_id,
            index=index,
            operation_id=operation_id,
            context_id=context_id,
            source_id=source_id,
            file_path=file_path,
            original_file_path=original_file_path,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )

        include_relocations: bool = self._plugin_params.custom_parameters.get(
            "include_relocations"
        )
        entropy_checks: bool = self._plugin_params.custom_parameters.get(
            "entropy_checks"
        )
        keep_warnings: bool = self._plugin_params.custom_parameters.get("keep_warnings")

        with pefile.PE(file_path) as pe:
            await self.process_record(
                pe,
                0,
                flt=flt,
                include_relocations=include_relocations,
                entropy_checks=entropy_checks,
                keep_warnings=keep_warnings,
            )

        return stats.status
