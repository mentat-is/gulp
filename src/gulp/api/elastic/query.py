import json

import muty.string
from pydantic import BaseModel, Field, model_validator

from gulp.api.elastic.structs import (
    GulpQueryFilter,
    gulpqueryflt_dsl_dict_empty,
    gulpqueryflt_to_dsl,
)

EXAMPLE_SIGMA_GROUP_FILTER = {
    "example": {
        "name": "test dummy APT",
        "expr": "(6ea858a8-ba71-4a12-b2cc-5d83312404c7 AND 74babdd6-a758-4549-9632-26535279e654)",
    }
}


class SigmaGroupFilter(BaseModel):
    """
    Allows filtering QueryResults against multiple sigma rules IDs ("id" in the YAML) using an expression supporting AND, OR, NOT, BEFORE, AFTER.
    this is useful to better spot inside a QueryResult array certain attacks.

    ### how to use SigmaGroupFilter:

    suppose you issue a query with query_sigma_zip (or whatever else API using Sigma rules) and you obtain a set of query results like this in your returned `GulpStats`:

    ~~~json
    [
    {
        "events": [
        {
            "@timestamp": -11644473600000,
            "_id": "93221cef42105c715ae62821df0ef88b28d4833ff3c8f6562cd78cc308f4fbc2966608aacdde3e3a8509d8bef3cf297a75655e529ba679164f400ee01bca6f0b"
        }
        ],
        "aggregations": null,
        "total_hits": 243,
        "search_after": [-11644473600000],
        "query_glyph_id": null,
        "error": null,
        "sigma_rule_file": "windows/create_remote_thread/create_remote_thread_win_uncommon_source_image.yml",
        "sigma_rule_id": "66d31e5f-52d6-40a4-9615-002d3789a119",
        "stored_query_id": null,
        "query_raw": null,
        "query_name": "Remote Thread Creation By Uncommon Source Image"
    },
    {
        "events": [
        {
            "@timestamp": 1593873086856,
            "_id": "89740a013f863b648be6450e98f716441c4534158928bbe9b6b360f48d55674fd22741783c592082aa64d3c41d12da2995f7e1c3a101fc245b42e3c02345b618"
        },
        {
            "@timestamp": 1594244521653,
            "_id": "bfd8657a2b385fee7e061b8774f47bfea03a9be9729a964af5dab6fa48543d13160c668efac417c8cd94cf65bfe9e79d09c69807d6a73643562e919f057308e9"
        }
        ],
        "aggregations": null,
        "total_hits": 7,
        "search_after": null,
        "query_glyph_id": null,
        "error": null,
        "sigma_rule_file": "windows/create_stream_hash/create_stream_hash_regedit_export_to_ads.yml",
        "sigma_rule_id": "0d7a9363-af70-4e7b-a3b7-1a176b7fbe84",
        "stored_query_id": null,
        "query_raw": null,
        "query_name": "Exports Registry Key To an Alternate Data Stream"
    },
    ]
    ~~~

    notice the `"sigma_rules_id"`, which is a unique identifier (from the YAML) for the Sigma rule that generated the query result.

    if you know that a specific attack uses the techniques mentioned by the above 2 rules, you can create a `SigmaGroupFilter` list like the following

    ~~~json
    [
    {
        // "name" may also be an id of a stored SigmaGroupFilter in the shared_data table
        "name": "known attack",
        // if "name" is an id of a stored SigmaGroupFilter, "expr" is ignored.
        // expr is a boolean expression supporting AND, OR, NOT, BEFORE, AFTER.
        "expr": "(0d7a9363-af70-4e7b-a3b7-1a176b7fbe84 AND 66d31e5f-52d6-40a4-9615-002d3789a119)"
    }
    ]
    ~~~

    and use it in the query APIs which supports `SigmaGroupFilter` to quickly spot if the attack is happening by looking for WsQueueDataType.SIGMA_GROUP_RESULT messages on the websocket :

    ~~~json
    {
        "type": 8,
        "username": "...",
        "ws_id": "...",
        "timestamp": 1234567890,
        "data": {
            "req_id": req_id,
            "sigma_group_results": [
                "known attack"
            ]
        }
    }
    ~~~


    for inner workings, read _evaluate() in query_utils.
    """

    name: str = Field(
        None,
        description='the name of the SigmaGroupFilter (i.e. "attack1"), or the ID of a stored one in the shared_data table (in such case, expr is ignored).',
    )
    expr: str = Field(
        None,
        description='the expression to evaluate, i.e. "(sigmaid1 AND sigmaid2) AND (sigmaid1 BEFORE sigmaid2) OR sigmaid3".',
    )

    def to_dict(self):
        return {"name": self.name, "expr": self.expr}

    @staticmethod
    def from_dict(d: dict):
        return SigmaGroupFilter(name=d["name"], expr=d["expr"])

    def __repr__(self) -> str:
        return f"SigmaGroupFilter(name={self.name}, expr={self.expr})"

    model_config = {"json_schema_extra": EXAMPLE_SIGMA_GROUP_FILTER}


class SigmaGroupFiltersParam(BaseModel):
    """
    the SigmaGroupFilterParam is a list of SigmaGroupFilter objects to filter the QueryResults.
    """

    sgf: list[SigmaGroupFilter] = Field(
        None,
        description="a list of SigmaGroupFilter objects to filter the QueryResults.",
    )

    @model_validator(mode="before")
    @classmethod
    def to_py_dict(cls, data: str | dict):
        if data is None or len(data) == 0:
            return {}

        if isinstance(data, dict):
            return data
        if isinstance(data, str):
            return json.loads(data)


class GulpQuery:
    """
    a lucene DSL query stored in the CollabObj table
    """

    def __init__(
        self,
        name: str,
        rule: dict,
        q_id: int = None,
        flt: GulpQueryFilter = None,
        glyph_id: int = None,
        sigma_rule_file: str = None,
        sigma_rule_text: str = None,
        sigma_rule_id: str = None,
        tags: list = None,
        description: str = None,
    ):
        """
        Initializes a GulpQuery object.

        Args:
            name (str): The name of the GulpQuery object.
            rule (dict): The rule (lucene DSL) associated with the GulpQuery object.
            q_id(int, optional): The query id (if the id is stored in the collab objects table). Defaults to None.
            flt (GulpQueryFilter): to restrict the query to a specific set of data. Defaults to None.
            glyph_id (int, optional): The id of the associated glyph (if any). Defaults to None.
            sigma_rule_file (str, optional): The original rule file path. Defaults to None.
            sigma_rule_text (str, optional): The original rule text. Defaults to None.
            sigma_rule_id (str, optional): The original rule id. Defaults to None.
            tags (list, optional): The tags associated with the GulpQuery object. Defaults to None.
            description (str, optional): The description of the GulpQuery object. Defaults to None.
        """
        self.name = name
        self.id = q_id
        self.sigma_rule_file = sigma_rule_file
        self.sigma_rule_id = sigma_rule_id
        self.sigma_rule_text = sigma_rule_text
        self.glyph_id = glyph_id
        self.tags = tags
        self.q = rule
        self.description = description

        if flt is not None:
            # combine the rule and filter to restrict the query to a specific set of data
            self.add_flt(flt)

    def add_flt(self, flt: GulpQueryFilter):
        """
        Add a filter to the GulpQuery object:
        this basically combines the rule and filter to restrict the query to a specific set of data

        Args:
            flt (GulpQueryFilter): The filter to add.
        """
        # first we convert the filter itself to a DSL query: the converted filter has a 'bool' query with a 'must' clause containing the filter.
        qq = gulpqueryflt_to_dsl(flt)
        if gulpqueryflt_dsl_dict_empty(qq):
            return

        total_q = {"query": {"bool": {"must": []}}}

        # append the filter to the 'must' clause
        flt_qs = qq["query"]["query_string"]["query"]
        if len(flt_qs) > 0:
            total_q["query"]["bool"]["must"].append(qq["query"])

        # and then we add the rule to the 'must' clause
        total_q["query"]["bool"]["must"].append(self.q["query"])
        self.q = total_q

    @staticmethod
    def from_dict(d: dict) -> "GulpQuery":
        """
        Create a GulpQuery object from a dictionary.

        Args:
            d (dict): The dictionary containing the GulpQuery attributes.

        Returns:
            GulpQuery: The created GulpQuery object.
        """
        return GulpQuery(
            d["name"],
            d["q"],
            d["id"],
            d["glyph_id"],
            d["tags"],
            d["sigma_rule_file"],
            d["sigma_rule_text"],
            d["sigma_rule_id"],
            d["description"],
        )

    def __repr__(self):
        return f"GulpQuery(name={self.name}, id={self.id}, q={muty.string.make_shorter(str(self.q))}, tags={self.tags}, glyph_id={self.glyph_id}), sigma_rule_file={self.sigma_rule_file}, sigma_rule_text={muty.string.make_shorter(str(self.sigma_rule_text))}, description={self.description}, sigma_rule_id={self.sigma_rule_id})"

    def to_dict(self):
        return self.__dict__


class QueryResult:
    """
    Represents a (single) query results in a GulpCollabType.QUERY stats object.
    """

    def __init__(
        self,
        events: list[dict] = None,
        stored_query_id: int = None,
        query_raw: dict = None,
        error: str = None,
        sigma_rule_file: str = None,
    ):
        self.events: list[dict] = events  # the matched events
        if self.events is None:
            self.events: list[dict] = []
        self.aggregations: dict = None  # the aggregations (if any)
        self.total_hits: int = 0
        self.search_after: list = None
        self.query_glyph_id: int = None
        self.error: str = error
        self.sigma_rule_file = sigma_rule_file
        self.sigma_rule_id: str = None
        self.stored_query_id: int = stored_query_id  # if the query is a stored query
        self.query_raw: dict = query_raw
        self.query_sigma_text: str = None  # only if query is a sigma query
        self.query_name: str = None
        self.req_id: str = None
        self.chunk: int = 0
        self.last_chunk: bool=False

    @staticmethod
    def from_dict(d: dict):
        """
        Creates a QueryResult object from a dictionary.

        Args:
            d (dict): A dictionary representation of the QueryResult object.

        Returns:
            QueryResult: A QueryResult object.
        """
        qr = QueryResult()
        qr.__dict__ = d
        return qr

    def to_dict(self) -> dict:
        """
        Converts the QueryResult object to a dictionary.

        Returns:
            dict: A dictionary representation of the QueryResult object.
        """
        return self.__dict__

    def __repr__(self) -> str:
        return f"QueryResult(events={self.events}, stored_query_id={self.stored_query_id}, query_name={self.query_name}, query_raw={self.query_raw}, query_sigma_text={self.query_sigma_text}, matched={self.total_hits > 0}, error={self.error}, sigma_rule_file={self.sigma_rule_file}, sigma_rule_id={self.sigma_rule_id}, chunk={self.chunk}, last_chunk={self.last_chunk}, req_id={self.req_id})"
