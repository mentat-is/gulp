import os
import re
from typing import Any, override

import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession
from gulp.api.collab.stats import (
    GulpRequestStats,
    PreviewDone,
    RequestCanceledError,
    SourceCanceledError,
)
from gulp.api.collab.structs import GulpRequestStatus
from gulp.api.opensearch.filters import GulpIngestionFilter
from gulp.api.opensearch.structs import GulpDocument
from gulp.structs import GulpPluginParameters
from gulp.plugin import GulpPluginBase, GulpPluginType


class Plugin(GulpPluginBase):

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.INGESTION]

    @override
    def desc(self) -> str:
        return "pfsense filter.log file processor."

    def display_name(self) -> str:
        return "pfsense"

    @override
    async def _record_to_gulp_document(
        self, record: Any, record_idx: int, **kwargs
    ) -> GulpDocument:

        event: str = record
        flent: dict = {}
        log_split: re.Match[str] = kwargs["match"]
        matches: tuple = log_split.groups()

        flent["time"] = matches[0]
        flent["host"] = matches[1]
        flent["rule"] = matches[2]

        rule_data = flent["rule"].split(",")
        field = 0
        fields = [
            "rulenum",
            "subrulenum",
            "anchor",
            "tracker",
            "realint",
            "interface",
            "reason",
            "act",
            "direction",
            "version",
        ]
        for f in range(len(fields)):
            # interface is realint converted to a friendlier format
            # since we can't convert the interface, we assing it the same value as realint'
            if fields[f] == "interface":
                flent[fields[f]] = rule_data[field - 1]
                continue
            flent[fields[f]] = rule_data[field]
            field += 1

        if flent["version"] == "4":
            fields = ["tos", "ecn", "ttl", "id", "offset", "flags", "protoid", "proto"]
            for f in range(len(fields)):
                flent[fields[f]] = rule_data[field]
                field += 1
        elif flent["version"] == "6":
            fields = ["class", "flowlabel", "hlim", "proto", "protoid"]
            for f in range(len(fields)):
                flent[fields[f]] = rule_data[field]
                field += 1
        else:
            # unknown IP version
            return None

        flent["lenght"] = rule_data[field]
        field += 1
        flent["srcip"] = rule_data[field]
        field += 1
        flent["dstip"] = rule_data[field]
        field += 1

        if flent["protoid"] in ["6", "17", "132"]:
            flent["srcport"] = rule_data[field]
            field += 1
            flent["dstport"] = rule_data[field]
            field += 1

            flent["src"] = f"{flent['srcip']}:{flent['srcport']}"
            flent["dst"] = f"{flent['dstip']}:{flent['dstport']}"

            flent["datalen"] = rule_data[field]
            field += 1

            if flent["protoid"] == "6":  # // TCP
                flent["tcpflags"] = rule_data[field]
                field += 1
                flent["seq"] = rule_data[field]
                field += 1
                flent["ack"] = rule_data[field]
                field += 1
                flent["window"] = rule_data[field]
                field += 1
                flent["urg"] = rule_data[field]
                field += 1
                flent["options"] = rule_data[field].split(";")
                field += 1
        elif flent["protoid"] in ["1", "58"]:
            flent["src"] = flent["srcip"]
            flent["dst"] = flent["dstip"]

            field += 1
            flent["icmp_type"] = rule_data[field]

            if flent["icmp_type"] in ["request", "reply"]:
                field += 1
                flent["icmp_id"] = rule_data[field]
                field += 1
                flent["icmp_seq"] = rule_data[field]
            elif flent["icmp_type"] == "unreachproto":
                field += 1
                flent["icmp_dstip"] = rule_data[field]
                field += 1
                flent["icmp_protoid"] = rule_data[field]
            elif flent["icmp_type"] == "unreachport":
                field += 1
                flent["icmp_dstip"] = rule_data[field]
                field += 1
                flent["icmp_protoid"] = rule_data[field]
                field += 1
                flent["icmp_port"] = rule_data[field]
                field += 1
            elif flent["icmp_type"] in [
                "unreach",
                "timexceed",
                "paramprob",
                "redirect",
                "maskreply",
            ]:
                field += 1
                flent["icmp_descr"] = rule_data[field]
            elif flent["icmp_type"] == "needfrag":
                field += 1
                flent["icmp_dstip"] = rule_data[field]
                field += 1
                flent["icmp_mtu"] = rule_data[field]
            elif flent["icmp_type"] == "tstamp":
                field += 1
                flent["icmp_id"] = rule_data[field]
                field += 1
                flent["icmp_seq"] = rule_data[field]
            elif flent["icmp_type"] == "tstampreply":
                field += 1
                flent["icmp_id"] = rule_data[field]
                field += 1
                flent["icmp_seq"] = rule_data[field]
                field += 1
                flent["icmp_otime"] = rule_data[field]
                field += 1
                flent["icmp_rtime"] = rule_data[field]
                field += 1
                flent["icmp_ttime"] = rule_data[field]
            else:
                field += 1
                flent["icmp_descr"] = rule_data[field]

        elif flent["protoid"] == "112":
            fields = ["type", "ttl", "vhid", "version", "advskew", "advbase"]
            for f in range(len(fields)):
                field += 1
                flent[fields[f]] = rule_data[field]

            flent["src"] = flent["srcip"]
            flent["dst"] = flent["dstip"]
        else:
            flent["src"] = flent["srcip"]
            flent["dst"] = flent["dstip"]

        # delete rule, as it contains the raw_event and it'd be duplicated
        del flent["rule"]

        d: dict = {}
        for k, v in flent.items():
            mapped = self._process_key(k, v)
            d.update(mapped)

        # map timestamp and event code manually
        d["@timestamp"] = flent["time"]
        d["event.code"] = flent["rulenum"]
        return GulpDocument(
            self,
            operation_id=self._operation_id,
            context_id=self._context_id,
            source_id=self._source_id,
            event_original=event,
            event_sequence=record_idx,
            log_file_path=self._original_file_path or os.path.basename(self._file_path),
            **d,
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
        plugin_params: GulpPluginParameters = None,
        flt: GulpIngestionFilter = None,
    ) -> GulpRequestStatus:
        try:
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
            )
            # taken from pfsense' parse_firewall_log_line function, these leve out some info (e.g. the pid of the filterlog process)
            rfc5424_pattern = r"<[0-9]{1,3}>[0-9]*\ (\S+?)\ (\S+?)\ filterlog\ \S+?\ \S+?\ \S+?\ (.*)$"
            rfc3164_pattern = r"(.*)\s(.*)\sfilterlog\[[0-9]+\]:\s(.*)$"

            rfc5424_regex: re.Pattern = re.compile(rfc5424_pattern)
            rfc3164_regex: re.Pattern = re.compile(rfc3164_pattern)
            regex: re.Pattern = None

            doc_idx = 0

            with open(file_path, "r") as log_file:
                # SNIPPET: peek first line and decide pattern to use
                # peek=log_file.readline()
                # self.pattern = self.rfc3164
                # if peek.startswith("<"):
                #    self.pattern = self.rfc5424
                # log_file.seek(0)

                for rr in log_file:
                    # pfsense does this check for every line in the log, we do too...
                    # is it because they expect mixed logs-formats? else we should just decide based
                    # on log's first line (see snippet above).
                    regex = rfc3164_regex
                    if rr.startswith("<"):
                        regex = rfc5424_regex

                    m: re.Match[str] = regex.match(rr)
                    if m:
                        try:
                            await self.process_record(
                                rr, doc_idx, flt=flt, match=m
                            )
                        except (RequestCanceledError, SourceCanceledError) as ex:
                            MutyLogger.get_instance().exception(ex)
                            await self._source_failed(ex)
                        except PreviewDone:
                            # preview done, stop processing
                            pass
                    else:
                        # no match
                        self._record_failed(
                            f"Regex ({self.pattern}) did not match line ({rr})"
                        )
                    doc_idx += 1

        except Exception as ex:
            await self._source_failed(ex)
        finally:
            await self._source_done(flt)
            return self._stats_status()
