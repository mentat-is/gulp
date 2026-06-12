import pytest

from gulp.api.opensearch.structs import GulpQueryHelpers


@pytest.mark.unit
def test_merge_queries_does_not_duplicate_existing_filter():
    existing = {
        "query": {
            "bool": {
                "filter": [
                    {"term": {"agent.type": "win_evtx"}},
                ]
            }
        }
    }
    incoming = {"query": {"term": {"agent.type": "win_evtx"}}}

    merged = GulpQueryHelpers.merge_queries(existing, incoming)

    assert merged == existing
