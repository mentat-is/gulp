"""
A WHOIS Enrichment Plugin for Gulp.

This plugin enriches documents with WHOIS information based on specified host fields.
It extracts potential IP addresses and hostnames from the input, performs WHOIS lookups, and returns enriched data.

It can handle both single entity lookups (like URLs or IPs) and generic text inputs containing multiple entities.
It supports custom parameters for field selection and output formatting.

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

import asyncio
import datetime
import ipaddress
import re
import socket
import orjson
import urllib
from typing import Any, Optional, override

import muty.dict
import muty.os
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
        module_name: str,
        pickled: bool = False,
        **kwargs,
    ) -> None:
        super().__init__(path, module_name, pickled=pickled, **kwargs)

        # stores results for the original_input string
        self._whois_cache: dict[str, Optional[dict[str, Any]]] = {}
        # stores raw whois data for individual resolved entities (ip/hostname)
        self._single_entity_whois_cache: dict[str, Optional[dict[str, Any]]] = {}

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
                desc="a list of ip fields to enrich: in every field, the plugin will look for (possibly multiple) IPV4/V6 addresses or hostnames to resolve and enrich with whois information.",
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
            ),
            GulpPluginCustomParameter(
                name="resolve_first_only",
                type="bool",
                desc="for hostnames, resolve only the first found IP address and do not attempt to resolve all possible addresses.",
                default_value=True,
            ),
        ]

    async def _get_raw_whois_for_entity(
        self, entity_key: str
    ) -> Optional[dict[str, Any]]:
        """
        fetches and processes raw whois information for a single resolved entity (ip or hostname).
        uses a cache to avoid redundant lookups for the same entity.

        Args:
            entity_key: the ip address or hostname string to lookup.
        Returns:
            a dictionary with flattened whois information, or none if an error occurs or no data.
        """
        MutyLogger.get_instance().debug(
            f"performing whois lookup for entity_key='{entity_key}'"
        )
        if entity_key in self._single_entity_whois_cache:
            MutyLogger.get_instance().debug(
                f"single entity whois cache hit for entity_key='{entity_key}'"
            )
            return self._single_entity_whois_cache[entity_key]

        try:

            # transform to ecs fields
            whois_info = IPWhois(entity_key).lookup_rdap(depth=1)
            # MutyLogger.get_instance().debug(f"raw whois_info for entity_key='{entity_key}': {whois_info}")

            # remove null fields and format datetime
            enriched_entity_data: dict[str, Any] = {}
            for k, v in muty.dict.flatten(whois_info).items():
                if isinstance(v, datetime.datetime):
                    v = v.isoformat()
                if v is not None:
                    # ensure we keep empty strings if they are actual values, but filter out none
                    enriched_entity_data[k] = v

            self._single_entity_whois_cache[entity_key] = enriched_entity_data
            return enriched_entity_data
        except Exception as ex:
            # log the exception and store None in the cache to avoid repeated lookups
            MutyLogger.get_instance().error(
                f"error during whois lookup for entity_key='{entity_key}': {ex}"
            )
            self._single_entity_whois_cache[entity_key] = None
            return None

    async def _extract_entities_with_regex(self, text_input: str) -> set[str]:
        """
        extracts potential ip addresses (v4 and v6), hostnames, and urls from a generic text string using regex.
        attempts to resolve hostnames to ip addresses.

        Args:
            text_input: the generic text string to parse.
        Returns:
            a set of unique resolved ip addresses or hostnames found in the text.
        """

        # MutyLogger.get_instance().debug(f"extracting entities with regex from: '{text_input[:100]}...'")
        entities_for_rdap: set[str] = set()

        # regex for ipv4 addresses
        ipv4_pattern: str = r"\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b"

        # regex for ipv6 addresses, based on python's ipaddress module internal regex
        # this pattern is designed to match valid ipv6 addresses more accurately for standalone ips.
        _hex4_re_str: str = r"[0-9a-fA-F]{1,4}"  # a hex quad
        _ipv6_parts: list[str] = [
            # 1:2:3:4:5:6:7:8
            r"(?:%s:){6}%s" % (_hex4_re_str, _hex4_re_str),
            # ::2:3:4:5:6:7
            r"::(?:%s:){5}%s" % (_hex4_re_str, _hex4_re_str),
            # (1)?::3:4:5:6:7
            r"(?:%s)?::(?:%s:){4}%s" % (_hex4_re_str, _hex4_re_str, _hex4_re_str),
            # (1:2)?::4:5:6:7
            r"(?:(?:%s:){0,1}%s)?::(?:%s:){3}%s"
            % (_hex4_re_str, _hex4_re_str, _hex4_re_str, _hex4_re_str),
            # (1:2:3)?::5:6:7
            r"(?:(?:%s:){0,2}%s)?::(?:%s:){2}%s"
            % (_hex4_re_str, _hex4_re_str, _hex4_re_str, _hex4_re_str),
            # (1:2:3:4)?::6:7
            r"(?:(?:%s:){0,3}%s)?::%s:%s"
            % (_hex4_re_str, _hex4_re_str, _hex4_re_str, _hex4_re_str),
            # (1:2:3:4:5)?::7
            r"(?:(?:%s:){0,4}%s)?::%s" % (_hex4_re_str, _hex4_re_str, _hex4_re_str),
            # (1:2:3:4:5:6)?::
            r"(?:(?:%s:){0,5}%s)?::" % (_hex4_re_str, _hex4_re_str),
            # ::
            r"::",
        ]
        # this pattern is for matching standalone ipv6 addresses
        ipv6_pattern: str = r"\b(?:%s)\b" % "|".join(_ipv6_parts)

        # regex for fqdn-like hostnames (simplified)
        # this pattern might also match parts of urls or ips, so validation is important
        hostname_pattern: str = (
            r"\b(?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}\b"
        )

        # define a simple pattern for ipv6 literals as they appear in urls (e.g., http://[::1]/)
        # this avoids embedding the complex 'ipv6_pattern' (designed for standalone ips) into the url regex,
        # which could lead to issues or incorrect matches for urls.
        _url_ipv6_literal_pattern: str = r"\[[0-9a-fA-F:]+\]"

        # regex for urls (simplified, focusing on host extraction).
        # captures the host part (fqdn, localhost, ipv4, or ipv6 literal) in group 1.
        # uses _url_ipv6_literal_pattern for matching ipv6 literals within urls.
        _url_pattern_template: str = (
            r"(?:https?://)?((?:[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,6}|localhost|\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b|%s)(?::[0-9]+)?(?:[/\?#]|$)"
        )
        url_pattern: str = _url_pattern_template % _url_ipv6_literal_pattern

        found_raw_entities: set[str] = set()

        # 1. extract hosts from urls
        # MutyLogger.get_instance().debug(f"url_pattern for extraction: {url_pattern}")
        for match in re.finditer(url_pattern, text_input, re.IGNORECASE):
            host_from_url: Optional[str] = match.group(1)
            if host_from_url:
                # MutyLogger.get_instance().debug(f"url regex matched: '{match.group(0)}', host: '{host_from_url}'")
                # remove brackets from ipv6 literals if present (e.g., "[::1]" -> "::1")
                if host_from_url.startswith("[") and host_from_url.endswith("]"):
                    host_from_url = host_from_url[1:-1]
                found_raw_entities.add(host_from_url.lower())

        # 2. extract ipv4 addresses
        for ip_match in re.finditer(ipv4_pattern, text_input):
            ip_str: str = ip_match.group(0)
            try:
                # validate if it's a real ip address
                ipaddress.ip_address(ip_str)
                found_raw_entities.add(ip_str)
            except ValueError:
                # MutyLogger.get_instance().warning(
                #     f"regex matched non-ip '{ip_str}' with ipv4_pattern, skipping."
                # )
                pass  # not a valid ip format

        # 3. extract ipv6 addresses (standalone)
        for ip_match in re.finditer(
            ipv6_pattern, text_input, re.IGNORECASE
        ):  # ignore case for hex in ipv6
            ip_str: str = ip_match.group(0)
            try:
                # validate if it's a real ipv6 address
                ipaddress.ip_address(ip_str)
                # MutyLogger.get_instance().debug(f"found potential standalone ipv6 address: '{ip_str}'")
                found_raw_entities.add(
                    ip_str.lower()
                )  # store ipv6 in lowercase canonical form
            except ValueError:
                # MutyLogger.get_instance().warning(
                #     f"regex matched non-ip '{ip_str}' with ipv6_pattern, skipping."
                # )
                pass  # not a valid ip format

        # 4. extract hostnames (that are not already identified as ips or url hosts)
        for host_match in re.finditer(hostname_pattern, text_input, re.IGNORECASE):
            hostname_str: str = host_match.group(0).lower()
            # avoid re-adding if already caught as part of a url or as an ip
            is_ip: bool = False
            try:
                ipaddress.ip_address(hostname_str)  # checks both ipv4 and ipv6
                is_ip = True
            except ValueError:
                pass  # not an ip

            if not is_ip:  # only add if it's not an ip (ips handled by ip_patterns)
                # further check if it was part of a url already processed to avoid redundant resolution.
                # this check is simplified: if the hostname_str is already in found_raw_entities,
                # it implies it was likely picked up by the url parser or ip parsers.
                if hostname_str not in found_raw_entities:
                    # MutyLogger.get_instance().debug(f"found potential standalone hostname: '{hostname_str}'")
                    found_raw_entities.add(hostname_str)
                # else:
                # MutyLogger.get_instance().debug(f"hostname '{hostname_str}' likely already extracted from a URL or as an IP.")

        MutyLogger.get_instance().debug(
            f"regex extraction found raw entities: {found_raw_entities}"
        )

        # 5. resolve hostnames to ips (if not already an ip) and add all valid ips to final set
        resolve_first_only: bool = self._plugin_params.custom_parameters.get(
            "resolve_first_only", True
        )

        for entity_str in found_raw_entities:
            try:
                # check if it's an ip (v4 or v6)
                ipaddress.ip_address(entity_str)
                entities_for_rdap.add(
                    entity_str.lower()
                )  # ensure ip is lowercase (for ipv6 canonical)
            except ValueError:
                # not an ip, so assumed hostname. attempt to resolve it.
                # MutyLogger.get_instance().debug(f"attempting to resolve hostname: '{entity_str}'")
                try:
                    addr_info_list: list[
                        tuple[
                            socket.AddressFamily,
                            socket.SocketKind,
                            int,
                            str,
                            tuple[str, Any],
                        ]
                    ] = await asyncio.get_event_loop().getaddrinfo(entity_str, None)
                    if addr_info_list:
                        # add all resolved ip addresses
                        for addr_info_tuple in addr_info_list:
                            resolved_ip: str = addr_info_tuple[4][
                                0
                            ]  # ip is the first element of sockaddr
                            entities_for_rdap.add(resolved_ip.lower())
                            # MutyLogger.get_instance().debug(f"resolved '{entity_str}' to '{resolved_ip}'")
                            if resolve_first_only:
                                # just use the first only
                                break
                    else:
                        # MutyLogger.get_instance().warning(
                        #     f"getaddrinfo returned empty for hostname: '{entity_str}', attempting rdap with hostname itself."
                        # )
                        entities_for_rdap.add(
                            entity_str.lower()
                        )  # add the hostname itself for rdap to try
                except socket.gaierror:
                    # MutyLogger.get_instance().warning(
                    #     f"could not resolve regex-extracted hostname: '{entity_str}' (gaierror), attempting rdap with hostname itself."
                    # )
                    entities_for_rdap.add(
                        entity_str.lower()
                    )  # add the hostname itself for rdap to try
                except Exception as e:
                    # MutyLogger.get_instance().error(
                    #     f"error resolving hostname '{entity_str}': {e}, attempting rdap with hostname itself."
                    # )
                    entities_for_rdap.add(entity_str.lower())  # add the hostname itself

        # MutyLogger.get_instance().debug(f"final entities for rdap after regex extraction and resolution: {entities_for_rdap}")
        return entities_for_rdap

    async def _get_whois(self, original_input: str) -> Optional[dict[str, Any]]:
        """
        Processes the original input string to extract entities (IPs or hostnames) and performs WHOIS lookups.

        input can be a URL, IP address, hostname, or generic text.

        It first checks if the input is a single entity (URL, IP, or simple hostname without spaces).
        If it is, it resolves the hostname to an IP if necessary and performs a WHOIS lookup.

        If the input contains spaces, it treats it as generic text and extracts potential entities using regex.
        It then performs WHOIS lookups for each unique entity found, filters the results based on custom parameters,
        and returns a dictionary with the enriched WHOIS data.

        If no entities are found or an error occurs, it caches None for the input and returns None.

        Args:
            original_input: The input string to process, which can be a URL, IP address, hostname, or generic text.

        Returns:
            A dictionary with enriched WHOIS data for the input, or None if no entities were found or an error occurred.

        """
        MutyLogger.get_instance().debug(
            f"requesting whois for input='{original_input[:100]}...'"
        )

        # check main cache for the entire original_input string
        if original_input in self._whois_cache:
            MutyLogger.get_instance().debug(
                f"main whois cache hit for input='{original_input[:100]}...'"
            )
            return self._whois_cache[original_input]

        final_entities_for_rdap: set[str] = set()
        is_single_entity_processing_path: bool = False

        # 1. determine if input is a single, direct entity candidate
        #    (url, ip, or simple hostname without spaces)
        single_target_entity: Optional[str] = None
        parsed_url: urllib.parse.ParseResult = urllib.parse.urlparse(original_input)
        if parsed_url.scheme and parsed_url.netloc:
            # looks like a full url
            # extract host, removing port if present
            host_from_url: str = parsed_url.netloc.split(":")[0].lower()
            if host_from_url.startswith("[") and host_from_url.endswith("]"):
                host_from_url = host_from_url[1:-1]
            single_target_entity = host_from_url
            is_single_entity_processing_path = True
            # MutyLogger.get_instance().debug(f"input recognized as url, target entity: '{single_target_entity}'")
        elif not re.search(r"\s", original_input):
            # no spaces, could be single ip or single hostname
            try:
                # check if it's an ip (v4 or v6)
                ipaddress.ip_address(original_input)
                single_target_entity = (
                    original_input.lower()
                )  # normalize ip case (for ipv6)
                is_single_entity_processing_path = True
                # MutyLogger.get_instance().debug(f"input recognized as ip: '{single_target_entity}'")
            except ValueError:
                # not an ip, check for hostname using only valid hostname characters
                if re.match(r"^[a-zA-Z0-9.-]+$", original_input):
                    single_target_entity = original_input.lower()
                    is_single_entity_processing_path = True
                    # MutyLogger.get_instance().debug(f"input recognized as potential single hostname: '{single_target_entity}'")
                else:
                    # not a clear single entity (e.g. "localhost", or just a word without dots)
                    MutyLogger.get_instance().warning(
                        f"input not a clear single entity (no spaces, but not ip/simple hostname): '{original_input[:100]}...'"
                    )
        else:
            # input contains spaces, will be treated as generic text for regex extraction
            MutyLogger.get_instance().warning(
                f"input contains spaces, treating as generic text: '{original_input[:100]}...'"
            )

        # 2. process based on whether it's a single entity path or generic text path
        if is_single_entity_processing_path and single_target_entity:
            # MutyLogger.get_instance().debug(f"processing as single entity: '{single_target_entity}'")
            # if it's already an ip, add it directly
            if self._is_ip_field(single_target_entity):
                final_entities_for_rdap.add(single_target_entity)
            else:
                # it's a hostname, try to resolve
                try:
                    # MutyLogger.get_instance().debug(f"resolving single entity hostname: {single_target_entity}")
                    resolved_ip: str = socket.gethostbyname(single_target_entity)
                    final_entities_for_rdap.add(resolved_ip)
                    # MutyLogger.get_instance().debug(f"resolved single entity hostname '{single_target_entity}' to '{resolved_ip}'")

                except socket.gaierror:
                    MutyLogger.get_instance().warning(
                        f"could not resolve single entity hostname: '{single_target_entity}', attempting rdap with hostname itself."
                    )
                    final_entities_for_rdap.add(
                        single_target_entity
                    )  # add hostname itself for rdap

            # if single entity processing yields no actual entity for lookup
            if not final_entities_for_rdap:
                MutyLogger.get_instance().warning(
                    f"single entity path for '{single_target_entity}' yielded no resolvable entities for rdap."
                )

        # if not a single entity path, then it's generic text path.
        if not is_single_entity_processing_path:
            # MutyLogger.get_instance().debug(f"treating input as generic text for regex extraction: '{original_input[:100]}...'")

            # extract entities using regex
            extracted_entities: set[str] = await self._extract_entities_with_regex(
                original_input
            )
            final_entities_for_rdap.update(extracted_entities)

        # if no entities were found by any method, cache none and return
        if not final_entities_for_rdap:
            # MutyLogger.get_instance().debug(f"no entities found for whois lookup in input: '{original_input[:100]}...'")
            self._whois_cache[original_input] = None
            return None

        # MutyLogger.get_instance().debug(f"final set of entities for rdap lookup: {final_entities_for_rdap}")

        # 3. perform whois for each unique entity and combine results
        final_combined_enriched_data: dict[str, Any] = {}
        data_for_unification: dict[str, dict[str, Any]] = {}

        for entity_to_lookup in final_entities_for_rdap:
            # _get_raw_whois_for_entity handles its own caching for the individual entity
            raw_enriched_data_for_entity: Optional[dict[str, Any]] = (
                await self._get_raw_whois_for_entity(entity_to_lookup)
            )

            if not raw_enriched_data_for_entity:
                MutyLogger.get_instance().warning(
                    f"no raw whois data returned for entity: {entity_to_lookup}"
                )
                continue  # skip to next entity

            # filter fields based on custom parameters ("whois_fields", "full_dump")
            to_keep: Optional[list[str]] = self._plugin_params.custom_parameters.get(
                "whois_fields"
            )
            full_dump: Optional[bool] = self._plugin_params.custom_parameters.get(
                "full_dump"
            )
            filtered_data_for_entity: dict[str, Any] = (
                self._filter_fields_with_wildcards(
                    raw_enriched_data_for_entity,
                    to_keep if to_keep else [],
                    full_dump if full_dump is not None else False,
                )
            )

            if not filtered_data_for_entity:
                MutyLogger.get_instance().warning(
                    f"data for entity {entity_to_lookup} is empty after filtering."
                )
                continue  # skip to next entity

            # MutyLogger.get_instance().debug(f"filtered data for entity {entity_to_lookup} has {len(filtered_data_for_entity)} fields.")

            # if unify_dump is enabled, store this entity's data for later unification
            if self._plugin_params.custom_parameters.get("unify_dump"):
                data_for_unification[entity_to_lookup] = filtered_data_for_entity
            else:
                # not unifying: prefix keys from filtered_data_for_entity with the entity itself (sanitized)
                # sanitize entity string for use as a prefix
                entity_prefix: str = re.sub(
                    r"[^a-zA-Z0-9_]", "_", str(entity_to_lookup)
                )
                for key, value in filtered_data_for_entity.items():
                    final_combined_enriched_data[f"{entity_prefix}_{key}"] = value

        # if unify_dump is true, create the 'unified_dump' field now
        if (
            self._plugin_params.custom_parameters.get("unify_dump")
            and data_for_unification
        ):
            # if it was a single entity path and resulted in one entity's data for unification
            if is_single_entity_processing_path and len(data_for_unification) == 1:
                # get the (only) key from data_for_unification, which is the entity string
                single_entity_data_key: str = list(data_for_unification.keys())[0]

                # dump only the data for that single entity
                ud: str = orjson.dumps(
                    data_for_unification[single_entity_data_key],
                    option=orjson.OPT_INDENT_2,
                ).decode()
                final_combined_enriched_data["unified_dump"] = ud

                MutyLogger.get_instance().debug(
                    f"unified dump created for single processed entity:\n{ud}"
                )
            else:
                # multiple entities found (either via regex or potentially complex single input that resolved to multiple, though less likely for single path)
                # or if it wasn't a single entity path but unify_dump is on
                # dump the dictionary of all entities and their data
                ud: str = orjson.dumps(
                    data_for_unification, option=orjson.OPT_INDENT_2
                ).decode()
                final_combined_enriched_data["unified_dump"] = ud
                MutyLogger.get_instance().debug(
                    f"unified dump created for {len(data_for_unification)} entities:\n{ud}"
                )

        # if after all processing, no data was added, cache none and return none
        if not final_combined_enriched_data:
            MutyLogger.get_instance().warning(
                f"no enrichment data produced for input: '{original_input[:100]}...'"
            )
            self._whois_cache[original_input] = None
            return None

        # cache the final combined result and return it
        # MutyLogger.get_instance().debug(f"whois enriched for input='{original_input[:100]}...', final data keys: {list(final_combined_enriched_data.keys())}")
        self._whois_cache[original_input] = final_combined_enriched_data
        return final_combined_enriched_data

    def _filter_fields_with_wildcards(
        self, flattened_enriched, whois_fields, full_dump=False
    ) -> dict:
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
            if pattern.endswith(".*"):
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
                            field: {"gte": "192.168.0.0", "lte": "192.168.255.255"}
                        }
                    },
                    {"range": {field: {"gte": "127.0.0.0", "lte": "127.255.255.255"}}},
                    {
                        "range": {
                            field: {"gte": "169.254.0.0", "lte": "169.254.255.255"}
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

    async def _enrich_documents_chunk(
        self,
        sess: AsyncSession,
        chunk: list[dict],
        chunk_num: int = 0,
        total_hits: int = 0,
        ws_id: str | None = None,
        user_id: str | None = None,
        req_id: str | None = None,
        operation_id: str | None = None,
        q_name: str | None = None,
        chunk_total: int = 0,
        q_group: str | None = None,
        last: bool = False,
        **kwargs,
    ) -> list[dict]:
        dd = []
        host_fields = self._plugin_params.custom_parameters.get("host_fields", [])

        # MutyLogger.get_instance().debug("host_fields: %s, num_docs=%d" % (host_fields, len(docs)))
        for doc in chunk:
            # TODO: when opensearch will support runtime mappings, this can be removed and done with "highlight" queries.
            # either, we may also add text mappings to ip fields in the index template..... but keep it as is for now...
            enriched: bool = False
            for host_field in host_fields:
                f = doc.get(host_field)
                if not f:
                    continue

                # append flattened whois data to the document
                whois_data = await self._get_whois(f)
                if whois_data:
                    enriched = True
                    for key, value in whois_data.items():
                        if value:
                            # also replace . with _ in the field name
                            doc[
                                "gulp.%s.%s.%s"
                                % (self.name, host_field.replace(".", "_"), key)
                            ] = value
            if enriched:
                # at least one host field was enriched
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
    ) -> tuple[int, int, list[str]]:
        # parse custom parameters
        await self._initialize(plugin_params)

        # build "should" nodes for each host field that match non-private IP addresses (both v4 and v6)
        host_fields = self._plugin_params.custom_parameters.get("host_fields", [])
        qq = {
            "query": {
                "bool": {
                    "should": [],
                    "minimum_should_match": 1,
                }
            }
        }
        for host_field in host_fields:
            qq["query"]["bool"]["should"].append(self._build_ip_query(host_field))

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
        return await super().enrich_single_document(
            sess, doc_id, operation_id, index, plugin_params
        )
