from fastapi import FastAPI
from fastapi.testclient import TestClient

from gulp.api.webui import add_webui_route


def test_webui_route_serves_assets_and_spa_fallback(tmp_path) -> None:
    """The packaged UI serves files without masking registered API routes."""
    (tmp_path / "static").mkdir()
    (tmp_path / "index.html").write_text("webui", encoding="utf-8")
    (tmp_path / "static" / "app.js").write_text("asset", encoding="utf-8")
    app = FastAPI()

    @app.get("/api")
    async def api() -> dict[str, bool]:
        return {"api": True}

    add_webui_route(app, tmp_path)
    client = TestClient(app)

    for path, body in (
        ("/api", '{"api":true}'),
        ("/static/app.js", "asset"),
        ("/operations/test", "webui"),
    ):
        response = client.get(path)
        print(path, response.status_code, response.text)
        assert response.status_code == 200
        assert response.text == body
