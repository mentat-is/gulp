"""
A WHOIS Enrichment Plugin for Gulp.

This plugin enriches documents with WHOIS information for IP addresses or hostnames found
in specified fields. It can look up WHOIS data for the given host_fields or use the default ones
(source.ip, destination.ip, host.hostname, and dns.question.name)

The plugin:
1. Extracts IP addresses or hostnames from specified fields
2. Looks up WHOIS information for each address
3. Adds the WHOIS data to the document with a prefix based on the plugin name and field
4. Caches results to avoid duplicate lookups

request body example:
{
    "flt": {
        "operation_ids": [
        "test_operation"
        ],
        "time_range": [ 1475739447131043840, 1475739547131043840 ]
    },
    "plugin_params": {
            // those fields will be looked up for whois information
            "host_fields": [ "source.ip", "destination.ip" ]
        }
}
"""

import datetime
import ipaddress
import socket
import json
import urllib
from typing import Optional, override

import muty.file
import muty.json
import muty.log
import muty.os
import muty.string
import muty.time
import muty.xml
from muty.log import MutyLogger
from ipwhois import IPWhois
from sqlalchemy.ext.asyncio import AsyncSession

from gulp.api.opensearch.filters import GulpQueryFilter
from gulp.plugin import GulpPluginBase, GulpPluginType
from gulp.structs import GulpPluginCustomParameter, GulpPluginParameters

muty.os.check_and_install_package("ipwhois", ">=1.3.0")


class Plugin(GulpPluginBase):
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
            ),
            GulpPluginCustomParameter(
                name="whois_fields",
                type="list",
                desc="list of whois fields to keep (only used if full_dump is set to false)",
                default_value=[
                    "asn_country_code",
                    "asn_description",
                    "network.end_address",
                    "network.country",
                    "objects.SN9171-RIPE.contact.name",
                    ],
            ),
            GulpPluginCustomParameter(
                name="full_dump",
                type="bool",
                desc="get all the whois information (ignore whois_fields)",
                default_value=False,
            ),
            GulpPluginCustomParameter(
                name="unify_dump",
                type="bool",
                desc="keep the whole enrichment in a single field",
                default_value=True,
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
        # MutyLogger.get_instance().debug("enriching whois for host=%s" % (host))
       

        try:
            enriched = {}
            if host in self._whois_cache:
                MutyLogger.get_instance().debug("whois cache hit for host=%s" % (host))
                enriched = self._whois_cache[host]
            else:
                # check if field is a url, if so extract the host
                netloc = urllib.parse.urlparse(host).netloc
                if netloc:
                    # netloc was extracted, we successfully parsed a URL
                    host = netloc.split(":")[0]

                # if the field is not an IP address, try to resolve it
                if not self._is_ip_field(host):
                    host = socket.gethostbyname(host)

                # Transform to ECS fields
                whois_info = IPWhois(host).lookup_rdap(depth=1)
                #MutyLogger.get_instance().debug("whois_info for host=%s: %s" % (host, whois_info))

                # remove null fields
                for k, v in muty.json.flatten_json(whois_info).items():
                    if isinstance(v, datetime.datetime):
                        v: datetime.datetime = v.isoformat()
                    if v:
                        enriched[k] = v
                # add to cache
                self._whois_cache[host] = enriched

            #print("enriched: %s" % enriched)

            # Filter fields based on custom parameters
            to_keep = self._plugin_params.custom_parameters.get("whois_fields")
            full_dump = self._plugin_params.custom_parameters.get("full_dump")
            enriched = self._filter_fields_with_wildcards(enriched, to_keep, full_dump)

            if self._plugin_params.custom_parameters.get("unify_dump"):
                enriched = {"unified_dump": json.dumps(enriched, indent=2)}

            MutyLogger.get_instance().debug("whois enriched for host=%s enriched=%s" % (host, enriched))
            return enriched
        
        except Exception as ex:
            MutyLogger.get_instance().exception(ex)
            self._whois_cache[host] = None
            return None

    def _filter_fields_with_wildcards(self, flattened_enriched, whois_fields, full_dump=False) -> dict:
        """
        Filter a flattened JSON object based on whois_fields patterns.
        
        Args:
            flattened_enriched: Already flattened JSON object (dict)
            whois_fields: List of fields to keep, supporting wildcards with ".*"
            full_dump: If True, return the original object without filtering
            
        Returns:
            Filtered flattened JSON object
        """
        if full_dump:
            return flattened_enriched
        
        # Preprocess patterns for faster matching
        exact_patterns = set()
        wildcard_prefixes = set()
        
        for pattern in whois_fields:
            if pattern.endswith('.*'):
                wildcard_prefixes.add(pattern[:-2])  # Store prefix without '.*'
            else:
                exact_patterns.add(pattern)
        
        # Create result with only matching keys
        result = {}
        for key, value in flattened_enriched.items():
            # Check exact match first (fastest check)
            if key in exact_patterns:
                result[key] = value
                continue
                
            # Then check wildcard matches
            for prefix in wildcard_prefixes:
                if key.startswith(prefix):
                    result[key] = value
                    break
                    
        return result

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
    ) -> int:
        # parse custom parameters
        await self._initialize(plugin_params)

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
        return await super().enrich_documents(
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
        await self._initialize(plugin_params)
        return await super().enrich_single_document(sess, doc_id, operation_id, index, plugin_params)
