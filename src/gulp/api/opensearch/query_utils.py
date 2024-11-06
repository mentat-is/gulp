import datetime
import os
from typing import Union

import muty.file
import muty.log
import muty.string
import muty.time
from dotwiz import DotWiz
from sigma.backends.opensearch import OpensearchLuceneBackend
from sigma.exceptions import SigmaError
from sigma.processing.pipeline import ProcessingPipeline
from sigma.rule import SigmaRule
from sqlalchemy.ext.asyncio import AsyncEngine

import gulp.api.collab_api as collab_api
import gulp.api.opensearch_api as opensearch_api
import gulp.config as config
import gulp.plugin as pluginbase
from gulp.api.collab.base import (
    GulpAssociatedEvent,
    GulpCollabFilter,
    GulpCollabType,
    GulpRequestStatus,
)
from gulp.api.collab.base import GulpCollabObject
from gulp.api.collab.stats import GulpStats
from gulp.api.elastic.query import (
    GulpQuery,
    QueryResult,
    SigmaGroupFilter,
    gulpqueryflt_to_elastic_dsl,
)
from gulp.api.elastic.structs import (
    QUERY_DEFAULT_FIELDS,
    GulpQueryFilter,
    GulpQueryOptions,
    GulpQueryParameter,
    GulpQueryType,
)
from gulp.defs import (
    GulpPluginType,
    InvalidArgument,
    ObjectAlreadyExists,
    ObjectNotFound,
)
from gulp.plugin_internal import GulpPluginGenericParams
from gulp.utils import logger


def _parse_expression(expr: str) -> tuple:
    if expr.startswith("("):
        matching_paren = _find_matching_paren(expr)
        left = _parse_expression(expr[1:matching_paren])
        rest = expr[matching_paren + 2 :].strip()
    elif " " in expr:
        left, rest = expr.split(" ", 1)
        left = ("id", left)
    else:
        return ("id", expr.strip()), ""
    if rest:
        if " " in rest:
            op, right = rest.split(" ", 1)
            if right.startswith("("):
                matching_paren = _find_matching_paren(right)
                right = _parse_expression(right[1:matching_paren])
            elif " " in right:
                right, _ = right.split(" ", 1)
                right = ("id", right)
            else:
                right = ("id", right.strip())
        else:
            op = rest
            right = None
        return (op.lower(), left, right)
    else:
        return left


def _find_matching_paren(expr: str) -> int:
    count = 0
    for i, c in enumerate(expr):
        if c == "(":
            count += 1
        elif c == ")":
            count -= 1
        if count == 0:
            return i
    return -1


def _find_sigma_rule(sigma_rule_id: str, qrs: list[dict]) -> dict:
    for qr in qrs:
        if qr.sigma_rule_id == sigma_rule_id:
            if len(qr.events) > 0:
                # we have a match
                return qr
    return None


def _evaluate(node: tuple, qrs: list[dict]) -> bool:
    """
    Evaluates a query node recursively and returns a boolean result.

    Args:
        node (tuple): The query node to evaluate.
        qrs (list[dict]): The list of QueryResult dicts to filter.

    Description:
        The `_evaluate` function is a recursive function that evaluates a query node and returns a boolean result. It takes two arguments: a tuple `node` representing the query node to evaluate, and a list `qrs` of QueryResult objects.

        The function checks the first element of the `node` tuple to determine the type of operation to perform:

        - If the operation is 'and', it recursively evaluates the second and third elements of the `node` tuple and returns the logical AND of the results.
        - If the operation is 'or', it does the same but returns the logical OR of the results.
        - If the operation is 'not', it recursively evaluates the second element of the `node` tuple and returns the logical NOT of the result.
        - If the operation is 'before', it finds the sigma rule for the second and third elements of the `node` tuple, gets the timestamps of their events, and checks if the minimum timestamp of the first is less than the maximum timestamp of the second.
        - If the operation is 'after', it does the same but checks if the maximum timestamp of the first is greater than the minimum timestamp of the second.
        - If the operation is 'id', it checks if any of the QueryResult objects in `qrs` have a sigma_rule_id that matches the second element of the `node` tuple.

        The `_find_sigma_rule` function is used to find a QueryResult object in `qrs` that has a sigma_rule_id matching the provided id. If no such object is found, it returns None.
    Returns:
        bool: The boolean result of the evaluation.
    """
    if node[0] == "and":
        return _evaluate(node[1], qrs) and _evaluate(node[2], qrs)
    elif node[0] == "or":
        return _evaluate(node[1], qrs) or _evaluate(node[2], qrs)
    elif node[0] == "not":
        return not _evaluate(node[1], qrs)
    elif node[0] == "before":
        qr1 = _find_sigma_rule(node[1][1], qrs)
        qr2 = _find_sigma_rule(node[2][1], qrs)
        if (
            qr1 is None
            or qr2 is None
            or qr1.get("events", None) is None
            or qr2.get("events", None) is None
        ):
            return False
        return min(e["@timestamp"] for e in qr1.events) <= max(
            e["@timestamp"] for e in qr2.events
        )
    elif node[0] == "after":
        qr1 = _find_sigma_rule(node[1][1], qrs)
        qr2 = _find_sigma_rule(node[2][1], qrs)
        if (
            qr1 is None
            or qr2 is None
            or qr1.get("events", None) is None
            or qr2.get("events", None) is None
        ):
            return False
        return max(e["@timestamp"] for e in qr1.events) >= min(
            e["@timestamp"] for e in qr2.events
        )
    elif node[0] == "id":
        return any(qr.sigma_rule_id == node[1] for qr in qrs)


async def preprocess_sigma_group_filters(
    flts: list[SigmaGroupFilter],
) -> list[SigmaGroupFilter]:
    """
    Preprocesses a list of expression filters and returns a list with the name of matching filters.

    Args:
        flts (list[SigmaGroupFilter]): A list of expression filters

    Returns:
        a list with the name of matching filters.
    """
    l = []
    for f in flts:
        try:
            # if it's a number, we try to get the shared data from the db
            shared_data_id = int(f.name)
            shared_data = await GulpCollabObject.get(
                await collab_api.session(),
                GulpCollabFilter(
                    type=[GulpCollabType.SHARED_DATA_SIGMA_GROUP_FILTER],
                    id=[shared_data_id],
                ),
            )
            shared_data = shared_data[0]
            d: dict = {"name": shared_data.name, "expr": shared_data.data["expr"]}
            flt = SigmaGroupFilter.from_dict(d)
            logger().debug(
                "sigma group filter with id=%d retrieved from db: %s"
                % (shared_data_id, flt)
            )
            l.append(flt)
        except ValueError:
            # it is a name, use directly
            l.append(f)

    return l


async def apply_sigma_group_filter(flt: SigmaGroupFilter, qrs: list[dict]) -> bool:
    """
    Applies an expression filter to a list of QueryResult from sigma rules.

    Args:
        bflt (SigmaGroupFilter): contains an expression to filter results based on sigma rule IDs, like "((66d31e5f-52d6-40a4-9615-002d3789a119 AND 0d7a9363-af70-4e7b-a3b7-1a176b7fbe84) AND (0d7a9363-af70-4e7b-a3b7-1a176b7fbe84 AFTER 66d31e5f-52d6-40a4-9615-002d3789a119)) OR 0f06a3a5-6a09-413f-8743-e6cf35561297".
            supported operators: AND, OR, BEFORE, AFTER (look at the _evaluate documentation)
        qrs (list[dict]): The list of QueryResult dicts to filter.

    Returns:
        list[QueryResult]: The filtered list of query results, or an empty list.
    """
    if not flt.expr.endswith(" "):
        # FIXME: this seems to fix a bug in the parser, to investigate ....
        flt.expr += " "
    tree = _parse_expression(flt.expr)
    qqrs = [DotWiz(qr) for qr in qrs]
    l = [qr for qr in qrs if _evaluate(tree, qqrs)]
    if len(l):
        # matched
        return True
    return False


async def apply_sigma_group_filters(
    flts: list[SigmaGroupFilter], qrs: list[dict]
) -> list[str]:
    """
    Applies a list of expression filters to a list of QueryResult from sigma rules and returns a list with the name of matching filters.

    Args:
        flts (list[SigmaGroupFilter]): A list of expression filters
        qrs (list[dict]): The list of QueryResult dicts to filter.

    Returns:
        a list with the name of matching filters.
    """

    l = []
    for f in flts:
        logger().debug("applying sigma group filter: %s ..." % (f))
        if await apply_sigma_group_filter(f, qrs):
            if f.name not in l:
                l.append(f.name)
    return l


async def check_canceled_or_failed(req_id: str) -> bool:
    # get stats and check if the request has been canceled/failed
    stats = await GulpStats.get(
        await collab_api.session(),
        GulpCollabFilter(req_id=[req_id]),
    )
    if stats is not None and stats[0].status in [
        GulpRequestStatus.FAILED,
        GulpRequestStatus.CANCELED,
    ]:
        logger().error("request %s failed or canceled!" % (req_id))
        return True

    return False


async def _create_notes_on_match(
    user_id: int,
    req_id: str,
    ws_id: str,
    gulp_query: GulpQuery,
    options: GulpQueryOptions,
    ws_api: str,
    query_res: QueryResult,
    raw_query_dict: dict,
    evts: list[dict],
    username: str,
) -> int:
    """
    create a note pinned on each matching event time start, with 'auto' tag

    Returns:
        int: -1 if the request has been interrupted
    """
    import gulp.api.rest.ws as ws_api
    from gulp.api.rest.ws import WsQueueDataType

    # get glyph_id
    glyph_id = gulp_query.glyph_id
    if glyph_id is None:
        glyph_id = options.notes_on_match_glyph_id

    num_notes = 0
    cs_batch: list[GulpCollabObject] = []
    failed: int = 0
    for e in evts:
        ev_id = e.get("_id")
        ev_ts = e.get("@timestamp")
        ev_operation_id = e.get("gulp.operation.id")
        ev_context = e.get("gulp.context")
        ev_logfile_path = e.get("log.file.path")

        # add a note pinned on event time start, with name=matching rule name, description=query dsl (and the other usual stuff...)
        text = query_res.query_name
        data = {
            "color": options.notes_on_match_color,
            "sigma_rule_id": query_res.sigma_rule_id,
            "title": gulp_query.name,
            "query_raw": raw_query_dict,
            "stored_query_id": query_res.stored_query_id,
        }

        try:
            description = gulp_query.sigma_rule_text
            # logger().debug("create note for event %s, id=%s, timestamp=%d, ws_id=%s" % (e, ev_id, ev_ts, ws_id))

            # create note on db
            cs = await GulpCollabObject.create(
                await collab_api.session(),
                token=None,
                req_id=req_id,
                name=gulp_query.name,
                t=GulpCollabType.NOTE,
                ws_id=ws_id,
                description=description,
                operation_id=ev_operation_id,
                context=ev_context,
                src_file=ev_logfile_path,
                txt=text,
                events=[GulpAssociatedEvent(id=ev_id, timestamp=ev_ts)],
                tags=["auto"],
                data=data,
                glyph_id=glyph_id,
                internal_user_id=user_id,
                skip_ws=True,
            )

            # batch for ws
            cs_batch.append(cs.to_dict())

        except ObjectAlreadyExists:
            # note is created only first time for a single event, in case of subsequent calls
            pass

        # every n notes, send batch of notes over ws and get stats and check if we must break out of loop (CANCELED or FAILED)
        num_notes += 1
        notes_on_match_batch_size = config.ws_notes_on_match_batch_size()
        if notes_on_match_batch_size > len(evts):
            notes_on_match_batch_size = len(evts)

        if num_notes % notes_on_match_batch_size == 0:
            # send batch over ws
            logger().error(
                "**NOT AN ERROR**: creating batch of notes on ws, num notes=%d"
                % (num_notes)
            )
            ws_api.shared_queue_add_data(
                WsQueueDataType.COLLAB_CREATE,
                req_id,
                {"collabs": cs_batch},
                username=username,
                ws_id=ws_id,
            )
            # reset cs_batch
            cs_batch = []

            # check if the request has been canceled
            if await check_canceled_or_failed(req_id):
                failed = -1
                break

    if len(cs_batch) > 0:
        logger().error(
            "**NOT AN ERROR**: creating LAST batch of notes on ws, num notes=%d"
            % (len(cs_batch))
        )

        # send last batch over ws
        ws_api.shared_queue_add_data(
            WsQueueDataType.COLLAB_CREATE,
            req_id,
            {"collabs": cs_batch},
            username=username,
            ws_id=ws_id,
        )
    logger().debug(
        "created %d notes for query_res name=%s, sigma_id=%s, num events=%d"
        % (
            num_notes,
            query_res.query_name,
            query_res.sigma_rule_id,
            len(query_res.events),
        )
    )
    return failed


async def query_by_gulpconvertedquery(
    user_id: int,
    username: str,
    req_id: str,
    ws_id: str,
    index: str,
    gulp_query: GulpQuery,
    options: GulpQueryOptions = None,
) -> QueryResult:
    """
    This is where the query actually happens: executes a raw_query using the converted GulpQuery

    Args:
        user_id (int): The ID of the user executing the query
        username (str): The name of the user executing the query
        req_id (str): The ID of the request.
        ws_id (str): The ID of the websocket.
        index (str): The name of the index to query.
        gulp_query (GulpQuery): The GulpQuery object containing the query DSL.
        options (GulpQueryOptions, optional): The GulpQueryOptions object containing additional query options. Defaults to None.
    Returns:
        QueryResult: The result of the query execution.
    """
    import gulp.api.rest.ws as ws_api
    from gulp.api.rest.ws import WsQueueDataType

    if options is None:
        options = GulpQueryOptions()

    logger().debug("gulp_query=%s" % (gulp_query))
    query_res = QueryResult()
    try:
        # add query name to result
        query_res.query_name = gulp_query.name
        if query_res.query_name is None:
            # use unique name
            query_res.query_name = "%s_%s" % (req_id, muty.string.generate_unique())

        # get raw query
        raw_query_dict: dict = gulp_query.q["query"]

        if gulp_query.id is not None:
            # we have the stored query id
            query_res.stored_query_id = gulp_query.id

        if options.include_query_in_result:
            # we may not include the query dsl and sigma rule text for smaller responses....
            # in every query_result also include the raw dsl query and matching sigma rule text
            query_res.query_raw = raw_query_dict
            query_res.query_sigma_text = gulp_query.sigma_rule_text

        # issue raw query
        # logger().debug("options.fields_filters=%s ..." % (options.fields_filter))
        res = None
        create_notes: bool = False
        search_after_loop: bool = True
        if gulp_query.sigma_rule_id is None:
            # non sigma query
            logger().debug("non-sigma query, collect events.")
            # options.disable_notes_on_match = True
        else:
            # sigma query
            if options.disable_notes_on_match:
                # disable notes on match for sigma queries, collect events instead (but no search_after loop)
                create_notes = False
                # search_after_loop = False
                logger().debug("disable_notes_on_match set for sigma query.")
            else:
                # create notes on match for sigma queries (default), do not collect events, search_after loop
                create_notes = True
                logger().debug("setting search_after_loop=True for sigma query.")

        try:
            # loop until search_after allows ...
            had_result: bool = False
            processed: int = 0
            chunk: int = 0
            while True:
                # logger().debug("calling query_raw from query_by_gulpconvertedquery ...")
                res: dict = None
                try:
                    res = await opensearch_api.query_raw(
                        opensearch_api.elastic(), index, raw_query_dict, options
                    )
                    had_result = True
                except Exception as ex:
                    logger().exception(ex)
                    if not had_result:
                        logger().error(
                            "NOT_FOUND sigma_rule_id=%s, ex=%s)"
                            % (gulp_query.sigma_rule_id, str(ex))
                        )
                    raise ex

                # get data
                evts = res.get("results", [])
                aggs = res.get("aggregations", None)
                len_evts = len(evts)

                # build a QueryResult
                query_res.req_id = req_id
                query_res.sigma_rule_file = gulp_query.sigma_rule_file
                query_res.sigma_rule_id = gulp_query.sigma_rule_id
                query_res.search_after = res.get("search_after", None)
                query_res.query_glyph_id = gulp_query.glyph_id
                query_res.total_hits = res.get("total", 0)
                logger().debug(
                    "%d results (TOTAL), this chunk=%d, for FOUND sigma_rule_id=%s"
                    % (query_res.total_hits, len_evts, query_res.sigma_rule_id)
                )

                query_res.events = evts
                query_res.aggregations = aggs
                if len_evts == 0 or len_evts < options.limit:
                    query_res.last_chunk = True

                # send QueryResult over websocket
                ws_api.shared_queue_add_data(
                    WsQueueDataType.QUERY_RESULT,
                    req_id,
                    query_res.to_dict(),
                    username=username,
                    ws_id=ws_id,
                )

                # processed an event chunk (evts)
                processed += len_evts
                chunk += 1
                logger().error(
                    "sent %d events to ws, num processed events=%d, chunk=%d ..."
                    % (len(evts), processed, chunk)
                )
                max_notes = config.query_sigma_max_notes()
                max_notes_reached = False
                if create_notes and not max_notes_reached:
                    logger().debug(
                        "creating notes for %d events, total hits=%d, sigma_id=%s, max_notes=%d, max_notes_reached=%r"
                        % (
                            len(evts),
                            query_res.total_hits,
                            query_res.sigma_rule_id,
                            max_notes,
                            max_notes_reached,
                        )
                    )
                    # create a note pinned on each matching event time start, with 'auto' tag
                    req_failed = await _create_notes_on_match(
                        user_id,
                        req_id,
                        ws_id,
                        gulp_query,
                        options,
                        ws_api,
                        query_res,
                        raw_query_dict,
                        evts,
                        username,
                    )
                    if req_failed == -1:
                        break
                else:
                    # check if the request has been canceled
                    # (if create_notes is true, check is done in _create_notes_on_match)
                    if await check_canceled_or_failed(req_id):
                        break

                if create_notes and max_notes > 0 and processed >= max_notes:
                    # limit the number of notes to create
                    logger().debug("max_notes=%d reached!" % (max_notes))
                    max_notes_reached = True

                if query_res.last_chunk:
                    logger().debug("last chunk, no more events found, query done!")
                    break

                # next batch of events (if any, and if search_after_loop is not disabled,i.e. on non-sigma queries)
                query_res.chunk += 1
                if not search_after_loop or query_res.search_after is None:
                    logger().debug(
                        "search_after=None or search_after_loop=False, query done!"
                    )
                    break

                options.search_after = query_res.search_after
                logger().debug(
                    "search_after=%s, total_hits=%d, running another query to get more results ...."
                    % (query_res.search_after, query_res.total_hits)
                )

        except ObjectNotFound as ex:
            # objectnotfound is not considered an error here
            if not had_result:
                logger().warning(
                    "ObjectNotFound, query_by_gulpconvertedquery: %s (query=%s)"
                    % (ex, raw_query_dict)
                )

        # done
    except Exception as ex:
        query_res.error = muty.log.exception_to_string(ex, with_full_traceback=True)

    return query_res


async def query_by_gulpqueryparam(
    user_id: int,
    username: str,
    req_id: str,
    ws_id: str,
    index: str,
    gqp: GulpQueryParameter,
    flt: GulpQueryFilter = None,
    options: GulpQueryOptions = None,
    sigma_rule_file: str = None,
    files_path: str = None,
    sgf: list[SigmaGroupFilter] = None,
) -> QueryResult:
    """
    Executes a query using the GulpQueryParameter and returns the QueryResult.

    NOTE: runs in a worker process with its own collab and elastic clients.

    Args:
        user_id (int): The user id who performs the query.
        username(str): The user name who performs the query.
        req_id (str): The request ID.
        ws_id (str): The websocket ID
        index (str): The elasticsearch index to query.
        gqp (GulpQueryParameter): The GulpQueryParameter object containing the query parameters.
        flt (GulpQueryFilter, optional): to restrict the query to just a subset of the data. Defaults to None.
        options (GulpQueryOptions, optional): Additional query options. Defaults to None.
        sigma_rule_file (str, optional): The original rule file path. Defaults to None.
        files_path (str, optional): Parent directory of the rule file. Defaults to None.
        sgf (list[SigmaGroupFilter], optional): A list of SigmaGroupFilter objects to filter the results. Defaults to None.
    Returns:
        QueryResult: The result of the query.

    Raises:
        Exception: If an error occurs during the query execution.
    """

    try:
        # turn GulpQueryParameter to elastic dsl so we can use query_raw
        gulp_query = await gulpqueryparam_to_gulpquery(
            await collab_api.session(),
            gqp,
            flt,
            sigma_rule_file=sigma_rule_file,
            files_path=files_path,
        )
    except Exception as ex:
        rex = muty.log.exception_to_string(ex, with_full_traceback=True)
        logger().error(rex)
        query_res = QueryResult()
        query_res.query_name = gqp.name
        query_res.error = rex
        return query_res

    # logger().debug('calling query_by_gulpconvertedquery ...')
    qres = await query_by_gulpconvertedquery(
        user_id, username, req_id, ws_id, index, gulp_query, options
    )

    if sgf is None:
        # events not needed
        qres.events = []
    else:
        # keep only timestamp (saves memory)
        qres.events = [{"@timestamp": e["@timestamp"]} for e in qres.events]

    return qres


async def sigma_to_raw(
    rule: str | SigmaRule,
    pysigma_plugin: str = None,
    plugin_params: GulpPluginGenericParams = None,
) -> tuple[SigmaRule, dict]:
    """
    Converts a Sigma rule to raw (Lucene) query for elasticsearch.

    Args:
        rule (str|SigmaRule): The Sigma rule, may be in YAML format to be converted using SigmaRule.from_yaml().
        pysigma_plugin (str, optional): optional sigma plugin name. if None, sigma rule logsource.product is used, and a plugin file named 'name' must be available.
            if pysigma_plugin loading fails, an empty pipeline is used.
        plugin_params (GulpPluginParams, optional): Additional parameters to pass to the plugin pipeline() function. Defaults to None.

    Returns:
        tuple[SigmaRule, dict]: A tuple containing the SigmaRule object and the Lucene DSL query.
    """

    # load sigma
    logger().debug(
        "converting sigma rule: %s ..." % (muty.string.make_shorter(str(rule)))
    )
    if isinstance(rule, SigmaRule):
        r = rule
    else:
        r = SigmaRule.from_yaml(rule)

    if pysigma_plugin is None:
        logger().warning(
            "pysigma_plugin is None, using sigma rule logsource.product=%s instead!"
            % (r.logsource.product)
        )
        if r.logsource.product is None:
            raise SigmaError(
                "Sigma rule %s has no product defined and pysigma_plugin is None!"
                % (rule)
            )

        pysigma_plugin = r.logsource.product

    # load pipeline via dedicated pysigma plugin
    try:
        # use provided pipeline
        mod: pluginbase.GulpPluginBase = pluginbase.load_plugin(
            pysigma_plugin, plugin_type=GulpPluginType.SIGMA
        )
        processing_pipeline = await mod.pipeline(plugin_params=plugin_params)
        pluginbase.unload_plugin(mod)

        backend = OpensearchLuceneBackend(processing_pipeline=processing_pipeline)

    except:
        # revert to empty pipeline
        logger().exception(
            'cannot load pysigma plugin "%s", reverting to empty pipeline.'
            % (pysigma_plugin)
        )
        backend = OpensearchLuceneBackend(processing_pipeline=ProcessingPipeline())

    # transform
    q = backend.convert_rule(r, "dsl_lucene")
    logger().info("converted rule: %s" % (q))
    if len(q) > 1:
        raise SigmaError(
            "Sigma rule %s generated more than one query (must fix!)" % (rule)
        )

    # get rule and dsl
    dsl = q[0]
    return r, dsl


async def gulpqueryparam_to_gulpquery(
    engine: AsyncEngine,
    g: GulpQueryParameter,
    flt: GulpQueryFilter = None,
    sigma_rule_file: str = None,
    files_path: str = None,
) -> GulpQuery:
    """
    Converts a GulpQueryParameter object to a GulpQuery (name, rule converted to dsl, optional further filter)

    Args:
        engine (AsyncEngine): The AsyncEngine object, used only for GulpQueryParameter.type=INDEX (either, may be None).
        g (GulpQueryParameter): The GulpQueryParameter object to convert.
        flt (GulpQueryFilter, optional): to restrict the query to just a subset of the data. Defaults to None.
        sigma_rule_file (str, optional): The original rule file path. Defaults to None.
        files_path (str, optional): Parent directory of the rule file. Defaults to None.

    Returns:
        GulpRule: The converted GulpRule object.

    Raises:
        InvalidArgument: If the GulpQueryParameter object has an invalid type.
    """

    if g.type == GulpQueryType.RAW:
        if g.name is None:
            raise InvalidArgument("name must be set for GulpQueryType.RAW")

        # already dsl
        logger().debug(
            "using existing dsl query: %s: %s, flt=%s ..." % (g.name, g.rule, flt)
        )
        r = g.rule.get("query", None)
        if r is not None:
            # extract query from { "query": { ... } format }
            # either, just use the query
            g.rule = r

        gq = GulpQuery(
            name=g.name, rule={"query": g.rule}, flt=flt, glyph_id=g.glyph_id
        )
        logger().debug("converted RAW to GulpQuery: %s" % (gq))
        return gq
    if g.type == GulpQueryType.SIGMA_YAML:
        sigma, dsl = await sigma_to_raw(g.rule, g.pysigma_plugin, g.plugin_params)
        if isinstance(g.rule, bytes):
            # ensure string
            g.rule = g.rule.decode("utf-8")

        # set tags in GulpQuery (use sigma rule tags + the ones already present in the GulpQuery object (no duplicates))
        tags = [str(x) for x in sigma.tags]
        if g.tags is not None:
            for x in g.tags:
                if x not in tags:
                    tags.append(x)

        if files_path is not None:
            # put filename into query struct aswell, so we can store the filename into collabobj table
            path_part = sigma_rule_file.split(files_path)[1]
            if path_part.startswith("/"):
                path_part = path_part[1:]
            sigma_rule_file = path_part

        return GulpQuery(
            name=sigma.title,
            rule=dsl,
            flt=flt,
            sigma_rule_file=sigma_rule_file,
            sigma_rule_text=str(g.rule),
            sigma_rule_id=str(sigma.id),
            tags=tags,
            glyph_id=g.glyph_id,
            description=sigma.description,
        )
    if g.type == GulpQueryType.GULP_FILTER:
        if g.name is None:
            raise InvalidArgument("name must be set for GulpQueryType.GULP_FILTER")
        r = gulpqueryflt_to_elastic_dsl(GulpQueryFilter.from_dict(g.rule))
        return GulpQuery(name=g.name, rule=r, glyph_id=g.glyph_id)
    if g.type == GulpQueryType.INDEX:
        # get from database
        o = await GulpCollabObject.get(
            engine,
            GulpCollabFilter(type=[GulpCollabType.STORED_QUERY], id=[int(g.rule)]),
        )
        tags = None
        sigma_rule_id = None
        sigma_rule_file = None
        sigma_description = None
        if o[0].data is not None:
            # if it is a sigma, get the id, tags and description
            tags = o[0].data.get("tags", None)
            sigma_rule_id = o[0].data.get("sigma_rule_id", None)
            sigma_rule_file = o[0].data.get("sigma_rule_file", None)
            sigma_description = o[0].data.get("description", None)
        return GulpQuery(
            name=o[0].name,
            rule=o[0].data["q"],
            q_id=o[0].id,
            flt=flt,
            glyph_id=g.glyph_id,
            description=sigma_description,
            tags=tags,
            sigma_rule_id=sigma_rule_id,
            sigma_rule_file=sigma_rule_file,
        )

    return InvalidArgument("invalid type: %s" % (g.type))


async def gulpqueryparams_to_gulpqueries(
    engine: AsyncEngine, g: list[GulpQueryParameter]
) -> list[GulpQuery]:
    """
        Converts a list of GulpQueryParameter objects to a list of GulpQuery objects.

        Args:
    engine (AsyncEngine): The AsyncEngine object, used only for GulpQueryParameter.type=INDEX (either, may be None).
                    g (list[GulpQueryParameter]): The list of GulpQueryParameter objects to convert.

        Returns:
            list[GulpRule]: The list of converted GulpRule objects.
    """
    l = []
    for q in g:
        r = await gulpqueryparam_to_gulpquery(engine, q)
        l.append(r)
    return l


def _tags_from_directories(start_directory: str, f: str) -> list[str]:
    """
    Extracts tags from directory name (/a/b/c -> ['a','b','c']).

    Args:
        start_directory (str): The start directory path
        f (str): The file path.

    Returns:
        list[str]: A list of tags or [] (no taggable path).
    """
    # get path after start directory(i.e. /tmp/aabbccdd/windows/builtin/rule.evtx -> windows/builtin)
    path_part = f.split(start_directory)[1]
    if path_part.startswith("/"):
        path_part = path_part[1:]
    if "/" not in path_part:
        return []

    tags = path_part.split("/")
    tags = tags[:-1]
    return tags


async def sigma_to_stored_query(
    engine: AsyncEngine,
    token: str,
    req_id: str,
    files_path: str,
    f: str,
    ws_id: str,
    pysigma_plugin: str = None,
    tags_from_directories: bool = True,
) -> Union[str, int]:
    """
    Converts a Sigma rule file to a stored query and creates a CollabObj.

    Args:
        token (str): The token for authentication.
        req_id (str): The request ID.
        files_path (str): The path to the files.
        f (str): The file to be converted.
        ws_id (str): The websocket ID
        pysigma_plugin (str, optional): optional sigma plugin name. if None, sigma rule logsource.product is used, and a plugin with that name must be available. Defaults to None.
        tags_from_directories (bool, optional): Whether to create tags from directories. Defaults to True.

    Returns:
        id of the created collab object or a string error message.
    """
    # take it from process
    engine = await collab_api.session()

    logger().debug("reading sigma file %s ..." % (f))
    content = await muty.file.read_file_async(f)

    # convert to gulprule
    try:
        gqp = GulpQueryParameter(
            rule=content, pysigma_plugin=pysigma_plugin, type=GulpQueryType.SIGMA_YAML
        )
        gr = await gulpqueryparam_to_gulpquery(
            engine, gqp, sigma_rule_file=f, files_path=files_path
        )

        tags = None
        if tags_from_directories:
            # create tags from directories, to extend already present tags (if any)
            tags = _tags_from_directories(files_path, f)
            if gr.tags is not None:
                tags.extend(gr.tags)
        else:
            tags = gr.tags

        # create stored query
        obj = await GulpCollabObject.create(
            engine,
            token,
            req_id,
            t=GulpCollabType.STORED_QUERY,
            ws_id=ws_id,
            name=gr.name,
            tags=tags,
            data=gr.to_dict(),
        )
        return obj.id
    except Exception as e:
        return "%s: %s" % (f, muty.log.exception_to_string(e))


async def stored_sigma_tags_to_gulpqueryparameters(
    collab: AsyncEngine,
    tags: list[str],
    gqp: list[GulpQueryParameter] = None,
) -> list[GulpQueryParameter]:
    """
    Converts a list of tags to GulpQueryParameters (uses tagged stored sigma queries).

    Args:
        collab (AsyncEngine): The collab engine.
        tags (list[str]): The list of tags.
        gqp (list[GulpQueryParameter], optional): The list of existing GulpQueryParameters (will be added to the returned list). Defaults to None.

    Returns:
        list[GulpQueryParameter]: The list of GulpQueryParameters.
    """
    flt = GulpCollabFilter(tags=tags, type=[GulpCollabType.STORED_QUERY])

    l = await GulpCollabObject.get(collab, flt)
    ll = []
    for n in l:
        # get sigma from stored query "data"
        if n.data is None:
            continue
        sigma: str = n.data.get("sigma_rule_text", None)
        if sigma is None:
            continue

        # add sigma
        g = GulpQueryParameter(rule=sigma, type=GulpQueryType.INDEX)
        ll.append(g)
    if gqp is not None:
        ll.extend(gqp)
    return ll


async def sigma_directory_to_gulpqueryparams(
    directory: str,
    pysigma_plugin: str = None,
    tags_from_directories: bool = True,
    plugin_params: GulpPluginGenericParams = None,
    tags_filter: list[str] = None,
    options: GulpQueryOptions = None,
) -> list[GulpQueryParameter]:
    """
    Converts Sigma YAML files in a directory to a list of GulpQueryParameter objects.

    Args:
        directory (str): The directory path containing the Sigma YAML files.
        pysigma_plugin (str, optional): fallback pysigma plugin `filename with or without .py/.pyc`. Defaults to None (use "logsource.product" from each sigma rule, if present).
        tags_from_directories (bool, optional): Whether to add (each sub)directory name as tags (plus the ones found in the sigma rule itself) in the resulting query. Defaults to True.
        plugin_params (GulpPluginParams, optional): Additional parameters to pass to the plugin pipeline() function. Defaults to None.
        tags_filter (list[str], optional): Only use sigma rules with these tags. Defaults to None (use all sigma rules).
    Returns:
        list[GulpQueryParameter]: A list of GulpQueryParameter objects.

    Raises:
        ObjectNotFound: If no Sigma files are found in the directory or no valid Sigma files are found in the directory.
    """

    files = await muty.file.list_directory_async(
        directory, recursive=True, mask="*.yml", files_only=True
    )
    if len(files) == 0:
        raise ObjectNotFound("no sigma files found in directory %s" % (directory))

    logger().debug("sigma files in directory %s: %s" % (directory, files))

    # read each file
    l = []
    for f in files:
        if os.path.basename(f).startswith("."):
            logger().warning("skipping (probably invalid) file %s ..." % (f))
            continue

        logger().debug("reading sigma file %s ..." % (f))

        # convert
        content = await muty.file.read_file_async(f)
        logger().debug("content: %s" % (muty.string.make_shorter(content.decode())))
        try:
            rr = SigmaRule.from_yaml(content)
        except Exception as e:
            # cannot parse this rule
            logger().exception("cannot parse sigma file %s: %s" % (f, e))
            continue

        # get rule tags
        if rr.tags is not None:
            tags = [str(x) for x in rr.tags]
        else:
            tags = []

        if tags_from_directories:
            # add tags from directories
            tt = _tags_from_directories(directory, f)
            for t in tt:
                if t not in tags:
                    tags.append(t)

        if tags_filter is not None and len(tags_filter) > 0:
            # check if tags_filter is a subset of the rule tags
            match = False
            for t in tags:
                if t in tags_filter:
                    match = True
                    break
            if not match:
                logger().debug(
                    "skipping sigma file %s, tags do not match: %s != %s ..."
                    % (f, rr.tags, tags_filter)
                )
                continue

        # all checks passed, accept this rule
        gqp = GulpQueryParameter(
            rule=content,
            name=rr.title,
            pysigma_plugin=pysigma_plugin,
            plugin_params=plugin_params,
            tags=tags,
            type=GulpQueryType.SIGMA_YAML,
        )
        l.append(gqp)

    if len(l) == 0:
        raise ObjectNotFound(
            "no (valid) sigma files found in directory %s" % (directory)
        )

    return l


def build_elastic_query_options(opt: GulpQueryOptions = None) -> dict:
    """
    Translates GulpQueryOptions into Elasticsearch query options.

    Args:
        opt (GulpQueryOptions, optional): The query options. Defaults to None (default options=sort by @timestamp desc).

    Returns:
        dict: The built query options.
    """
    logger().debug(opt)
    if opt is None:
        # use default
        opt = GulpQueryOptions()

    n = {}
    if opt.sort is not None:
        # add sort options
        n["sort"] = []
        for k, v in opt.sort.items():
            n["sort"].append({k: {"order": v}})
        # we also add event.hash and event.sequence among sort options to return very distinct results (sometimes, timestamp granularity is not enogh)
        if "event.hash" not in opt.sort:
            n["sort"].append({"event.hash": {"order": "asc"}})
        if "event.sequence" not in opt.sort:
            n["sort"].append({"event.sequence": {"order": "asc"}})
    else:
        # default sort
        n["sort"] = [
            {"@timestamp": {"order": "asc"}},
            {"event.hash": {"order": "asc"}},
            {"event.sequence": {"order": "asc"}},
        ]

    # fields filter
    n["source"] = adjust_fields_filter(QUERY_DEFAULT_FIELDS, opt.fields_filter)

    if opt.limit is not None:
        # use provided
        n["size"] = opt.limit
    else:
        # default
        n["size"] = 1000

    if opt.search_after is not None:
        # next chunk from this point
        n["search_after"] = opt.search_after
    else:
        n["search_after"] = None

    # logger().debug("query options: %s" % (json.dumps(n, indent=2)))
    return n


def process_event_timestamp(
    evt: dict,
    timestamp_offset_msec: int = 0,
    timestamp_is_string: bool = True,
    timestamp_format_string: str = None,
    timestamp_day_first: bool = False,
    timestamp_year_first: bool = True,
    timestamp_unit: str = "ms",
    timestamp_field: str = "@timestamp",
) -> int:
    """
    return the timestamp in nanoseconds, accounting for the various timestamp formats and units.

    Args:
        evt (dict): the event to process.
        timestamp_offset_msec (int): the timestamp offset to apply, in milliseconds.
        timestamp_is_string (bool): if True, the timestamp is a string.
        timestamp_format_string (str): the timestamp format string (or None to autodetect).
        timestamp_day_first (bool): if True, the timestamp is a string and the day is the first element in the string.
        timestamp_year_first (bool): if True, the timestamp is a string and the year is the first element in the string.
        timestamp_unit (str): the timestamp unit: can be "s" (seconds from epoch) or "ms" (milliseconds from epoch).
        timestamp_field (str): the timestamp field.

    Returns:
        int: the timestamp in nanoseconds.
    """

    # get value first
    t = evt.get(timestamp_field)

    # turn to nanoseconds
    if timestamp_is_string:
        # parse string timestamp
        if timestamp_format_string:
            # use format string
            t = datetime.datetime.strptime(t, timestamp_format_string)
            t = muty.time.datetime_to_epoch_nsec(t)
        else:
            # try to auto detect format
            t = muty.time.string_to_epoch_nsec(
                t, dayfirst=timestamp_day_first, yearfirst=timestamp_year_first
            )
    else:
        # already a number
        t = int(t)

        # convert to nanoseconds
        if timestamp_unit == "ms":
            t = t * muty.time.MILLISECONDS_TO_NANOSECONDS
        elif timestamp_unit == "s":
            t = t * muty.time.SECONDS_TO_NANOSECONDS

    if timestamp_offset_msec != 0:
        # apply offset
        t = t + timestamp_offset_msec * muty.time.MILLISECONDS_TO_NANOSECONDS
    return t


def adjust_fields_filter(
    mandatory_list: list[str], fields_filter: list[str] = None
) -> list[str]:
    """
    Adjust the fields filter to include the mandatory fields if not already present.

    if fields_filter is None, the default mandatory fields are returned.
    if fields_filter is ["*"], None is returned (no fields filter, all fields must be returned).

    Args:
        mandatory_list (list[str]): The list of mandatory fields
        fields_filter (list[str]): The fields filter

    Returns:
        list[str]: The adjusted fields filter string
    """
    if fields_filter is not None and len(fields_filter) > 0:
        if fields_filter[0] == "*":
            # return all fields
            return None

        # we need at least the mandatory fields
        ff = fields_filter
        for f in mandatory_list:
            if f not in ff:
                ff.append(f)
    else:
        # no fields filter, use default
        ff = mandatory_list

    return ff
