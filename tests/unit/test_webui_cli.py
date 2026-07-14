import os

from gulp import webui_cli


def test_webui_launcher_serves_bundled_build_with_pnpm(tmp_path, monkeypatch) -> None:
    """The packaged launcher runs pnpm serve against the bundled UI."""
    (tmp_path / "index.html").write_text("webui", encoding="utf-8")
    executed: list[str] = []

    monkeypatch.setattr(webui_cli, "bundled_webui_path", lambda: tmp_path)
    monkeypatch.setattr(webui_cli.shutil, "which", lambda command: f"/bin/{command}")
    monkeypatch.setattr(
        webui_cli.os,
        "execv",
        lambda _executable, command: executed.extend(command),
    )

    assert webui_cli.main(["--host", "127.0.0.1", "--port", "3100"]) == 0
    print("web UI launch command:", executed)
    assert executed == [
        "/bin/pnpm",
        "dlx",
        "serve@14.2.5",
        "-s",
        os.fspath(tmp_path),
        "-l",
        "tcp://127.0.0.1:3100",
        "--no-port-switching",
        "--no-clipboard",
    ]
