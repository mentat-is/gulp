import datetime
import ipaddress
import socket
import urllib
from typing import Optional, override

import muty.file
import muty.json
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from ipwhois import IPWhois
from muty.log import MutyLogger
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.api.opensearch.query import GulpQueryHelpers, GulpQueryParameters
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

muty.os.check_and_install_package("ipwhois", ">=1.3.0")


class Plugin(GulpPluginBase):
    """
        a whois enrichment plugin

        body example:

        {
        "flt": {
            "operation_ids": [
            "test_operation"
            ],
            "int_filter": [ 1475739447131043840, 1475739547131043840 ]
        },
        "plugin_params": {
                // those fields will be looked up for whois information
                "host_fields": [ "source.ip", "destination.ip" ]
            }
        }

        example enriched document:

        {
      "status": "success",
      "timestamp_msec": 1739040830294,
      "req_id": "test_req",
      "data": {
        "@timestamp": "2016-10-06T07:37:27.131044+00:00",
        "gulp.timestamp": 1475739447131043840,
        "gulp.operation_id": "test_operation",
        "gulp.context_id": "66d98ed55d92b6b7382ffc77df70eda37a6efaa1",
        "gulp.source_id": "fabae8858452af6c2acde7f90786b3de3a928289",
        "log.file.path": "/gulp/samples/win_evtx/security_big_sample.evtx",
        "agent.type": "win_evtx",
        "event.original": "{\n  \"Event\": {\n    \"#attributes\": {\n      \"xmlns\": \"http://schemas.microsoft.com/win/2004/08/events/event\"\n    },\n    \"System\": {\n      \"Provider\": {\n        \"#attributes\": {\n          \"Name\": \"Microsoft-Windows-Security-Auditing\",\n          \"Guid\": \"{54849625-5478-4994-a5ba-3e3b0328c30d}\"\n        }\n      },\n      \"EventID\": 5156,\n      \"Version\": 0,\n      \"Level\": 0,\n      \"Task\": 12810,\n      \"Opcode\": 0,\n      \"Keywords\": \"0x8020000000000000\",\n      \"TimeCreated\": {\n        \"#attributes\": {\n          \"SystemTime\": \"2016-10-06T07:37:27.131044Z\"\n        }\n      },\n      \"EventRecordID\": 239152,\n      \"Correlation\": null,\n      \"Execution\": {\n        \"#attributes\": {\n          \"ProcessID\": 4,\n          \"ThreadID\": 80\n        }\n      },\n      \"Channel\": \"Security\",\n      \"Computer\": \"WIN-WFBHIBE5GXZ.example.co.jp\",\n      \"Security\": null\n    },\n    \"EventData\": {\n      \"ProcessID\": 1428,\n      \"Application\": \"\\\\device\\\\harddiskvolume1\\\\windows\\\\system32\\\\dns.exe\",\n      \"Direction\": \"%%14593\",\n      \"SourceAddress\": \"192.168.16.1\",\n      \"SourcePort\": \"49782\",\n      \"DestAddress\": \"202.12.27.33\",\n      \"DestPort\": \"53\",\n      \"Protocol\": 17,\n      \"FilterRTID\": 66305,\n      \"LayerName\": \"%%14611\",\n      \"LayerRTID\": 48\n    }\n  }\n}",
        "event.sequence": 12027,
        "event.code": "5156",
        "gulp.event_code": 5156,
        "event.duration": 1,
        "gulp.unmapped.Guid": "{54849625-5478-4994-a5ba-3e3b0328c30d}",
        "gulp.unmapped.Task": 12810,
        "gulp.unmapped.Keywords": "0x8020000000000000",
        "gulp.unmapped.SystemTime": "2016-10-06T07:37:27.131044Z",
        "winlog.record_id": "239152",
        "process.pid": 1428,
        "process.thread.id": 80,
        "winlog.channel": "Security",
        "winlog.computer_name": "WIN-WFBHIBE5GXZ.example.co.jp",
        "process.executable": "\\device\\harddiskvolume1\\windows\\system32\\dns.exe",
        "network.direction": "%%14593",
        "source.ip": "192.168.16.1",
        "source.port": 49782,
        "destination.ip": "202.12.27.33",
        "destination.port": 53,
        "network.transport": "17",
        "gulp.unmapped.FilterRTID": 66305,
        "gulp.unmapped.LayerName": "%%14611",
        "gulp.unmapped.LayerRTID": 48,
        "_id": "e3b579719b5a49f0f27b3d17fd376ba2",
        "gulp.enrich_whois.destination_ip.nir.query": "202.12.27.33",
        "gulp.enrich_whois.destination_ip.asn_registry": "apnic",
        "gulp.enrich_whois.destination_ip.asn": "7500",
        "gulp.enrich_whois.destination_ip.asn_cidr": "202.12.27.0/24",
        "gulp.enrich_whois.destination_ip.asn_country_code": "JP",
        "gulp.enrich_whois.destination_ip.asn_date": "1997-03-04",
        "gulp.enrich_whois.destination_ip.asn_description": "M-ROOT-DNS WIDE Project, JP",
        "gulp.enrich_whois.destination_ip.query": "202.12.27.33",
        "gulp.enrich_whois.destination_ip.network.handle": "202.12.27.0 - 202.12.27.255",
        "gulp.enrich_whois.destination_ip.network.status.0": "active",
        "gulp.enrich_whois.destination_ip.network.remarks.0.title": "description",
        "gulp.enrich_whois.destination_ip.network.remarks.0.description": "root DNS server",
        "gulp.enrich_whois.destination_ip.network.notices.0.title": "Source",
        "gulp.enrich_whois.destination_ip.network.notices.0.description": "Objects returned came from source\nAPNIC",
        "gulp.enrich_whois.destination_ip.network.notices.1.title": "Terms and Conditions",
        "gulp.enrich_whois.destination_ip.network.notices.1.description": "This is the APNIC WHOIS Database query service. The objects are in RDAP format.",
        "gulp.enrich_whois.destination_ip.network.notices.1.links.0": "http://www.apnic.net/db/dbcopyright.html",
        "gulp.enrich_whois.destination_ip.network.notices.2.title": "Whois Inaccuracy Reporting",
        "gulp.enrich_whois.destination_ip.network.notices.2.description": "If you see inaccuracies in the results, please visit: ",
        "gulp.enrich_whois.destination_ip.network.notices.2.links.0": "https://www.apnic.net/manage-ip/using-whois/abuse-and-spamming/invalid-contact-form",
        "gulp.enrich_whois.destination_ip.network.links.0": "https://rdap.apnic.net/ip/202.12.27.0/24",
        "gulp.enrich_whois.destination_ip.network.links.1": "https://netox.apnic.net/search/202.12.27.0%2F24?utm_source=rdap&utm_medium=result&utm_campaign=rdap_result",
        "gulp.enrich_whois.destination_ip.network.events.0.action": "registration",
        "gulp.enrich_whois.destination_ip.network.events.0.timestamp": "2008-09-04T06:49:25Z",
        "gulp.enrich_whois.destination_ip.network.events.1.action": "last changed",
        "gulp.enrich_whois.destination_ip.network.events.1.timestamp": "2020-06-22T05:50:44Z",
        "gulp.enrich_whois.destination_ip.network.start_address": "202.12.27.0",
        "gulp.enrich_whois.destination_ip.network.end_address": "202.12.27.255",
        "gulp.enrich_whois.destination_ip.network.cidr": "202.12.27.0/24",
        "gulp.enrich_whois.destination_ip.network.ip_version": "v4",
        "gulp.enrich_whois.destination_ip.network.type": "ASSIGNED PORTABLE",
        "gulp.enrich_whois.destination_ip.network.name": "NSPIXP-2",
        "gulp.enrich_whois.destination_ip.network.country": "JP",
        "gulp.enrich_whois.destination_ip.entities.0": "AK3",
        "gulp.enrich_whois.destination_ip.entities.1": "IRT-WIDE-JP",
        "gulp.enrich_whois.destination_ip.entities.2": "ORG-WA3-AP",
        "gulp.enrich_whois.destination_ip.objects.AK3.handle": "AK3",
        "gulp.enrich_whois.destination_ip.objects.AK3.links.0": "https://rdap.apnic.net/entity/AK3",
        "gulp.enrich_whois.destination_ip.objects.AK3.events.0.action": "registration",
        "gulp.enrich_whois.destination_ip.objects.AK3.events.0.timestamp": "2008-09-04T07:29:47Z",
        "gulp.enrich_whois.destination_ip.objects.AK3.events.1.action": "last changed",
        "gulp.enrich_whois.destination_ip.objects.AK3.events.1.timestamp": "2013-08-24T00:01:43Z",
        "gulp.enrich_whois.destination_ip.objects.AK3.roles.0": "technical",
        "gulp.enrich_whois.destination_ip.objects.AK3.roles.1": "administrative",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.name": "Akira Kato",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.kind": "individual",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.address.0.value": "Keio University\nGraduate School of Media Design\n4-1-1 Hiyoshi, Kohoku, Yokohama 223-8526",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.phone.0.type": "voice",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.phone.0.value": "+81 45 564 2490",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.phone.1.type": "fax",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.phone.1.value": "+81 45 564 2503",
        "gulp.enrich_whois.destination_ip.objects.AK3.contact.email.0.value": "kato@wide.ad.jp",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.handle": "IRT-WIDE-JP",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.remarks.0.title": "remarks",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.remarks.0.description": "irt-admin@wide.ad.jp was validated on 2024-08-06\nabuse@wide.ad.jp was validated on 2024-12-06",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.links.0": "https://rdap.apnic.net/entity/IRT-WIDE-JP",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.events.0.action": "registration",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.events.0.timestamp": "2011-01-10T04:23:42Z",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.events.1.action": "last changed",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.events.1.timestamp": "2024-12-10T22:36:35Z",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.roles.0": "abuse",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.contact.name": "IRT-WIDE-JP",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.contact.kind": "group",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.contact.address.0.value": "Keio University\n5322 Endo Fujisawa 252-8520",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.contact.email.0.value": "irt-admin@wide.ad.jp",
        "gulp.enrich_whois.destination_ip.objects.IRT-WIDE-JP.contact.email.1.value": "abuse@wide.ad.jp",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.handle": "ORG-WA3-AP",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.links.0": "https://rdap.apnic.net/entity/ORG-WA3-AP",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.events.0.action": "registration",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.events.0.timestamp": "2017-08-08T23:29:26Z",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.events.1.action": "last changed",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.events.1.timestamp": "2023-09-05T02:14:46Z",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.roles.0": "registrant",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.name": "WIDE",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.kind": "org",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.address.0.value": "Murai Lab Keio University\n5322 Endo, Fujisawa-shi",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.phone.0.type": "voice",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.phone.0.value": "+81-466-49-3529",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.phone.1.type": "fax",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.phone.1.value": "+81-466-49-1101",
        "gulp.enrich_whois.destination_ip.objects.ORG-WA3-AP.contact.email.0.value": "junsec@sfc.wide.ad.jp"
      }
    }
    """

    def __init__(
        self,
        path: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(path, pickled=pickled, **kwargs)
        self._whois_cache = {}

    def type(self) -> list[GulpPluginType]:
        return [GulpPluginType.ENRICHMENT]

    def display_name(self) -> str:
        return "enrich_whois"

    @override
    def desc(self) -> str:
        return "whois enrichment plugin"

    @override
    def custom_parameters(self) -> list[GulpPluginCustomParameter]:
        return [
            GulpPluginCustomParameter(
                name="host_fields",
                type="list",
                desc="a list of ip fields to enrich.",
                default_value=[
                    "source.ip",
                    "destination.ip",
                    "host.hostname",
                    "dns.question.name",
                ],
            )
        ]

    async def _get_whois(self, host: str) -> Optional[dict]:
        """
        get whois information for an host address.

        this function also caches the results to avoid multiple lookups for the same host.

        Args:
            ip: host address string
        Returns:
            Whois information as a dictionary or None if not found
        """
        if host in self._whois_cache:
            return self._whois_cache[host]

        try:
            # check if field is a url, if so extract the host
            netloc = urllib.parse.urlparse(host).netloc
            if netloc:
                # netloc was extracted, we successfully parsed a URL
                host = netloc

            # if the field is not an IP address, try to resolve it
            if not self._is_ip_field(host):
                host = socket.gethostbyname(host)

            # Transform to ECS fields
            whois_info = IPWhois(host).lookup_rdap(depth=1)

            # remove null fields
            enriched = {}
            for k, v in muty.json.flatten_json(whois_info).items():
                if isinstance(v, datetime.datetime):
                    v: datetime.datetime = v.isoformat()

                enriched[k] = v

            # add to cache
            self._whois_cache[host] = enriched
            return enriched

        except Exception:
            self._whois_cache[host] = None
            return None

    def _is_ip_field(self, field_value: str) -> bool:
        """Check if a field value is an IP address (v4 or v6)"""
        try:
            ipaddress.ip_address(field_value)
            return True
        except ValueError:
            return False

    def _build_ip_query(self, field: str) -> dict:
        """Build a query that matches valid IP addresses while excluding private/local ranges"""
        return {
            "bool": {
                "must": [{"exists": {"field": field}}],
                "must_not": [
                    # IPv4 private/local ranges
                    {"range": {field: {"gte": "10.0.0.0", "lte": "10.255.255.255"}}},
                    {"range": {field: {"gte": "172.16.0.0", "lte": "172.31.255.255"}}},
                    {
                        "range": {
                            field: {"gte": "192.168.0.0",
                                    "lte": "192.168.255.255"}
                        }
                    },
                    {"range": {field: {"gte": "127.0.0.0", "lte": "127.255.255.255"}}},
                    {
                        "range": {
                            field: {"gte": "169.254.0.0",
                                    "lte": "169.254.255.255"}
                        }
                    },
                    {"range": {field: {"gte": "224.0.0.0", "lte": "239.255.255.255"}}},
                    # IPv6 private/local ranges
                    {
                        "range": {
                            field: {
                                "gte": "fc00::",
                                "lte": "fdff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                            }
                        }
                    },
                    {
                        "range": {
                            field: {
                                "gte": "fe80::",
                                "lte": "febf:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                            }
                        }
                    },
                    {"term": {field: "::1"}},
                    {
                        "range": {
                            field: {
                                "gte": "ff00::",
                                "lte": "ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff",
                            }
                        }
                    },
                ],
            }
        }

    async def _enrich_documents_chunk(self, docs: list[dict], **kwargs) -> list[dict]:
        dd = []
        host_fields = self._plugin_params.custom_parameters.get(
            "host_fields", [])
        # MutyLogger.get_instance().debug("host_fields: %s, num_docs=%d" % (host_fields, len(docs)))
        for doc in docs:
            # TODO: when opensearch will support runtime mappings, this can be removed and done with "highlight" queries.
            # either, we may also add text mappings to ip fields in the index template..... but keep it as is for now...
            for host_field in host_fields:
                f = doc.get(host_field)
                if not f:
                    continue

                # append flattened whois data to the document
                whois_data = await self._get_whois(f)
                if whois_data:
                    for key, value in whois_data.items():
                        if value:
                            # also replace . with _ in the field name
                            doc[
                                "gulp.%s.%s.%s"
                                % (self.name, host_field.replace(".", "_"), key)
                            ] = value
                    dd.append(doc)
                    self._tot_enriched += 1

        return dd

    @override
    async def enrich_documents(
        self,
        sess: AsyncSession,
        user_id: str,
        req_id: str,
        ws_id: str,
        operation_id: str,
        index: str,
        flt: GulpQueryFilter = None,
        plugin_params: GulpPluginParameters = None,
        **kwargs,
    ) -> None:
        # parse custom parameters
        self._initialize(plugin_params)

        # build queries for each host field that match non-private IP addresses (both v4 and v6)
        host_fields = self._plugin_params.custom_parameters.get(
            "host_fields", [])
        qq = {
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1,
                }
            }
        }
        for host_field in host_fields:
            qq["query"]["bool"]["should"].append(
                self._build_ip_query(host_field))

        # MutyLogger.get_instance().debug("query: %s" % qq)
        await super().enrich_documents(
            sess, user_id, req_id, ws_id, operation_id, index, flt, plugin_params, rq=qq
        )

    @override
    async def enrich_single_document(
        self,
        sess: AsyncSession,
        doc_id: str,
        operation_id: str,
        index: str,
        plugin_params: GulpPluginParameters,
    ) -> dict:

        # parse custom parameters
        self._initialize(plugin_params)
        return await super().enrich_single_document(sess, doc_id, operation_id, index, plugin_params)
