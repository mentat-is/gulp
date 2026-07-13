def normalize_elastic_fieldname(
    field_name: str,
    replace_with="",
    null_field_prefix="null_field",
    is_nested_field=True,
) -> str:
    """
    Check if a given single field name contains restricted characters.
    Do not use this function to normalize nested field names ("some.field.name").

    Elastic requires the field name to be:
        - Lowercase only
        - Cannot include \, /, *, ?, ", <, >, |, space (the character, not the word), ,, #
        - Indices prior to 7.0 could contain a colon (:), but that's been deprecated and won't be supported in 7.0+
        - Cannot start with -, _, +
        - Cannot be . or ..
        - Cannot be longer than 255 characters

    Args:
        field_name (str): the field name to normalize.
        replace_with (str, option): the character to replace bad characters with. Default ''.
        null_field_prefix (str, optional): how to name the field if it is empty.
        is_nested_field (bool,  optional): if the field is part of a nested field, skip unecessary checks

    """
    field_name = field_name.lower()

    if len(field_name) > 255:
        field_name = field_name[:255]

    for bad_char in ["\\", "/", "*", "?", '"', "'", "<", ">", " ", ",", "#", "."]:
        field_name = field_name.replace(bad_char, replace_with)

    # TODO: technically if we are a nested field these checks should not be needed
    # if not is_nested_field:
    for bad_char in ["_", "-", "+"]:
        if field_name.startswith(bad_char):
            # TODO find a more elegant solution, please?
            field_name = field_name.replace(bad_char, replace_with, 1)
            field_name = normalize_elastic_fieldname(
                field_name, replace_with, null_field_prefix
            )

    if not field_name:
        field_name = null_field_prefix

    return field_name


def build_field_exist_query(field: str) -> dict:
    return {"bool": {"must": [{"exists": {"field": field}}]}}


def build_skip_private_ip_query(field: str) -> dict:
    """Build a query that matches valid IP addresses while excluding private/local ranges"""
    return {
        "bool": {
            "must": [{"exists": {"field": field}}],
            "must_not": [
                # IPv4 private/local ranges
                {"range": {field: {"gte": "10.0.0.0", "lte": "10.255.255.255"}}},
                {"range": {field: {"gte": "172.16.0.0", "lte": "172.31.255.255"}}},
                {"range": {field: {"gte": "192.168.0.0", "lte": "192.168.255.255"}}},
                {"range": {field: {"gte": "127.0.0.0", "lte": "127.255.255.255"}}},
                {"range": {field: {"gte": "169.254.0.0", "lte": "169.254.255.255"}}},
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
