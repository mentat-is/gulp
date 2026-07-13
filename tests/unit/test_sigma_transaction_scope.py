from unittest.mock import AsyncMock, MagicMock

import pytest

from gulp.api.opensearch import sigma
from gulp.api.collab.source import GulpSource


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sigma_conversion_releases_source_transaction_before_file_reads(
    monkeypatch,
):
    """Do not hold a PostgreSQL read transaction during Sigma conversion."""
    sess = AsyncMock()
    source = MagicMock(id="source-1", mapping_parameters=None)
    monkeypatch.setattr(GulpSource, "get_by_filter", AsyncMock(return_value=[source]))

    async def _isfile(_path: str) -> bool:
        sess.rollback.assert_awaited_once_with()
        return False

    monkeypatch.setattr(sigma.aiofiles.os.path, "isfile", _isfile)

    queries = await sigma.sigmas_to_queries(
        sess,
        "user-1",
        "operation-1",
        ["rule.yml"],
        paths=True,
    )

    assert queries == []
    sess.rollback.assert_awaited_once_with()
