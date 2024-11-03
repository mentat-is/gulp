import datetime
import os
import re

import muty.dict
import muty.jsend
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml

from gulp.api.collab.base import GulpRequestStatus
from gulp.api.collab.stats import TmpIngestStats
from gulp.api.elastic.structs import GulpDocument, GulpIngestionFilter
from gulp.api.mapping.models import GulpMappingField, GulpMapping
from gulp.defs import GulpLogLevel, GulpPluginType
from gulp.plugin import GulpPluginBase
from gulp.plugin_internal import GulpPluginGenericParams


class Plugin(GulpPluginBase):
    def _normalize_loglevel(self, l: int | str) -> str:
        """
        int to str mapping
        :param l:
        :return:
        """

        ll = int(l)
        if ll == journal.LOG_DEBUG:
            return GulpLogLevel.VERBOSE
        elif ll == journal.LOG_INFO:
            return GulpLogLevel.INFO
        elif ll == journal.LOG_WARNING:
            return GulpLogLevel.WARNING
        elif ll == journal.LOG_ERR:
            return GulpLogLevel.ERROR
        elif ll == journal.LOG_CRIT:
            return GulpLogLevel.CRITICAL
        else:
            # shouldnt happen
            return GulpLogLevel.ALWAYS

    def type(self) -> GulpPluginType:
        return GulpPluginType.INGESTION

    def desc(self) -> str:
        return "pfsense filter.log file processor."

    def display_name(self) -> str:
        return "pfsense"

    def version(self) -> str:
        return "1.0"

    async def _record_to_gulp_document(
        self,
        operation_id: int,
        client_id: int,
        context: str,
        source: str,
        fs: TmpIngestStats,
        record: any,
        record_idx: int,
        custom_mapping: GulpMapping = None,
        index_type_mapping: dict = None,
        plugin: str = None,
        plugin_params: GulpPluginGenericParams = None,
        extra: dict = None,
        **kwargs,
    ) -> list[GulpDocument]:

        event: str = record
        if extra is None:
            extra = {}

        flent = {}
        log_split = self.pattern.match(event)
        matches = log_split.groups()

        all = event
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

        fme: list[GulpMappingField] = []
        for k, v in flent.items():
            # each event item is a list[str]
            e = self._map_source_key(plugin_params, custom_mapping, k, v, **kwargs)
            for f in e:
                fme.append(f)

        time_nanosec = muty.time.string_to_epoch_nsec(flent["time"])
        time_msec = muty.time.nanos_to_millis(time_nanosec)

        docs = self._build_gulpdocuments(
            fme,
            idx=record_idx,
            timestamp_nsec=time_nanosec,
            timestamp=time_msec,
            operation_id=operation_id,
            context=context,
            plugin=self.display_name(),
            client_id=client_id,
            raw_event=event,
            original_id=str(record_idx),
            event_code=flent["rulenum"],
            src_file=os.path.basename(source),
            # gulp_log_level=gulp_log_level,
            # original_log_level=original_log_level,
        )

        return docs

    async def ingest_file(
        self,
        index: str,
        req_id: str,
        client_id: int,
        operation_id: int,
        context: str,
        source: str | list[dict],
        ws_id: str,
        plugin_params: GulpPluginGenericParams = None,
        flt: GulpIngestionFilter = None,
        **kwargs,
    ) -> GulpRequestStatus:

        # taken from pfsense' parse_firewall_log_line function, these leve out some info (e.g. the pid of the filterlog process)
        rfc5424_pattern = (
            r"<[0-9]{1,3}>[0-9]*\ (\S+?)\ (\S+?)\ filterlog\ \S+?\ \S+?\ \S+?\ (.*)$"
        )
        rfc3164_pattern = r"(.*)\s(.*)\sfilterlog\[[0-9]+\]:\s(.*)$"

        self.rfc5424 = re.compile(rfc5424_pattern)
        self.rfc3164 = re.compile(rfc3164_pattern)
        self.pattern: re.Pattern = None

        await super().ingest_file(
            index=index,
            req_id=req_id,
            client_id=client_id,
            operation_id=operation_id,
            context=context,
            source=source,
            ws_id=ws_id,
            plugin_params=plugin_params,
            flt=flt,
            **kwargs,
        )
        fs = TmpIngestStats(source)

        ev_idx = 0

        # initialize mapping
        try:
            index_type_mapping, custom_mapping = await self._initialize()(
                index, source, mapping_file="pfsense.json", plugin_params=plugin_params
            )
        except Exception as ex:
            fs = self._source_failed(fs, source, ex)
            return await self._finish_ingestion(
                index, source, req_id, client_id, ws_id, fs=fs, flt=flt
            )

        try:
            with open(source, "r") as log_file:
                # SNIPPET: peek first line and decide pattern to use
                # peek=log_file.readline()
                # self.pattern = self.rfc3164
                # if peek.startswith("<"):
                #    self.pattern = self.rfc5424
                # log_file.seek(0)

                for rr in log_file:
                    try:
                        # pfsense does this check for every line in the log, we do too...
                        # is it because they expect mixed logs-formats? else we should just decide based
                        # on log's first line (see snippet above).
                        self.pattern = self.rfc3164
                        if rr.startswith("<"):
                            self.pattern = self.rfc5424

                        if self.pattern.match(rr) is None:
                            fs = self._record_failed(
                                fs,
                                rr,
                                source,
                                f"Regex ({self.pattern}) did not match line ({rr})",
                            )
                            continue

                        fs, must_break = await self.process_record(
                            index,
                            rr,
                            ev_idx,
                            self._record_to_gulp_document,
                            ws_id,
                            req_id,
                            operation_id,
                            client_id,
                            context,
                            source,
                            fs,
                            custom_mapping=custom_mapping,
                            index_type_mapping=index_type_mapping,
                            plugin_params=plugin_params,
                            flt=flt,
                            **kwargs,
                        )
                        ev_idx += 1
                        if must_break:
                            break
                    except Exception as ex:
                        fs = self._record_failed(fs, rr, source, ex)

        except Exception as ex:
            fs = self._source_failed(fs, source, ex)

        # done
        return await self._finish_ingestion(
            index, source, req_id, client_id, ws_id, fs=fs, flt=flt
        )
