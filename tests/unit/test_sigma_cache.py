import asyncio

import pytest

from gulp.api.opensearch import sigma


class _FakeRedis:
    def __init__(self) -> None:
        self.data: dict[str, bytes] = {}
        self.nx_failures = 0
        self.set_calls: list[tuple[str, int | None, bool]] = []

    async def get(self, key: str) -> bytes | None:
        return self.data.get(key)

    async def set(
        self,
        key: str,
        value: bytes,
        *,
        ex: int | None = None,
        nx: bool = False,
    ) -> bool:
        self.set_calls.append((key, ex, nx))
        if nx and key in self.data:
            self.nx_failures += 1
            return False
        self.data[key] = value
        return True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_sigma_batch_cache_coalesces_concurrent_misses(monkeypatch) -> None:
    """Only one caller converts a batch while a peer waits for completion."""
    redis = _FakeRedis()
    monkeypatch.setattr(
        sigma.GulpRedis,
        "get_instance",
        lambda: type("Redis", (), {"client": lambda self: redis})(),
    )
    monkeypatch.setattr(sigma, "_SIGMA_CACHE_WAIT_SEC", 0)

    assert await sigma._cache_get_or_claim("batch") is None
    waiter = asyncio.create_task(sigma._cache_get_or_claim("batch"))
    while not redis.nx_failures:
        await asyncio.sleep(0)

    await sigma._cache_set("batch", {"complete": True})

    assert await waiter == {"complete": True}
    assert ("batch:lock", 60, True) in redis.set_calls
    assert ("batch", 3600, False) in redis.set_calls


def test_sigma_batch_cache_key_ignores_rule_order() -> None:
    """Equivalent batches share a completion marker regardless of file order."""
    first = sigma._make_sigma_batch_cache_key(["rule-a", "rule-b"])
    second = sigma._make_sigma_batch_cache_key(["rule-b", "rule-a"])

    assert first == second
    assert first.startswith("gulp:sigma_cache:batch:")
