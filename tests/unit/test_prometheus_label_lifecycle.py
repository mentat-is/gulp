"""Prometheus label cardinality regressions."""

import pytest
from fastapi import APIRouter, FastAPI
from fastapi.testclient import TestClient

from gulp.api import prometheus_api
from gulp.api.prometheus_api import GulpMetrics


@pytest.mark.unit
def test_request_stats_ongoing_uses_only_bounded_scope_label():
    labels = GulpMetrics.request_stats_ongoing._labelnames

    print("request_stats_ongoing labels:", labels)
    assert labels == ("scope",)


@pytest.mark.unit
def test_http_metrics_use_route_selected_by_fastapi_0139(monkeypatch):
    config = type(
        "Config",
        (),
        {
            "prometheus_enabled": staticmethod(lambda: True),
            "prometheus_endpoint": staticmethod(lambda: "/metrics"),
        },
    )()
    monkeypatch.setattr(
        prometheus_api.GulpConfig,
        "get_instance",
        staticmethod(lambda: config),
    )
    app = FastAPI()
    router = APIRouter()

    @router.get("/included-route")
    async def _included_route():
        return {"ok": True}

    app.include_router(router)
    prometheus_api.setup_prometheus(app)
    counter = GulpMetrics.http_requests_total.labels(
        handler="/included-route", method="GET", status="2xx"
    )
    before = counter._value.get()

    with TestClient(app) as client:
        response = client.get("/included-route")

    print("included route response:", response.status_code, response.text)
    assert response.status_code == 200
    assert counter._value.get() == before + 1
