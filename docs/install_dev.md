# Development installation

Run the backend from a repository checkout, with the services in Docker on
your machine. Use the devcontainer, or set things up by hand.

Either way, start the services on the host first. The `dev` profile adds
Adminer, Elasticvue and Redis Insight, to look inside them:

```bash
git clone https://github.com/mentat-is/gulp.git && cd gulp
docker compose --profile dev up -d
```

## Devcontainer

The repository has a [devcontainer](https://containers.dev/) for VS Code.

1. Install the
   [Dev Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers).
2. Clone [gulp-sdk](https://github.com/mentat-is/gulp-sdk) next to `gulp/`:
   the container installs it in editable mode. The devcontainer also
   bind-mounts the other folders listed under `mounts` in
   [devcontainer.json](../.devcontainer/devcontainer.json); create the ones
   you don't have as empty folders.
3. Open [gulp.code-workspace](../gulp.code-workspace) and choose **Reopen in
   Container**. The container is built the first time only, and creates
   `.venv` with gULP installed in editable mode.
4. In a terminal inside the container, run `gulp`.

The container is based on Debian 13 (Trixie). When its base image changes,
rebuild it; if `.venv` holds native extensions built for the old base, delete
`.venv` and run `.devcontainer/devsetup.sh` again. The setup script also
recreates a stale `.venv` and removes package metadata left by older versions.

## By hand

Requirements: Python 3.12 or 3.13, Git, and Docker. For example, on Arch
Linux `sudo pacman -S python git docker docker-compose`; on Debian or Ubuntu
`sudo apt install python3-venv python3-dev build-essential git`, plus Docker.

```bash
cd gulp
python3 -m venv .venv && . .venv/bin/activate
pip install -e .
gulp
```

The first run creates `~/.config/gulp/gulp_cfg.json` from
[the template](../src/gulp/gulp_cfg_template.json), the collaboration
database and the `test_operation` operation. The default user is `admin` /
`admin`, and the API reference is at <http://localhost:8080/docs>. To start
again from scratch, run `gulp --reset-collab`.

To work on the SDK at the same time, clone
[gulp-sdk](https://github.com/mentat-is/gulp-sdk) next to `gulp/` and run
`pip install -e ../gulp-sdk`.

Extra plugins and mapping files are plain files: copy or link them into
`~/.config/gulp/plugins` and `~/.config/gulp/mapping_files`.

## Web UI

Run the UI with live reload as described in the
[web UI development guide](webui/install.md).

## More

- [Testing](testing.md)
- [Troubleshooting](troubleshooting.md)

### Coding style

- Format with `black`, default options.
- Use these `pylint` settings in VS Code's `settings.json`:

  ```json
  {
    "pylint.args": [
      "--jobs=0",
      "--disable=useless-parent-delegation",
      "--disable=no-member",
      "--disable=no-name-in-module",
      "--disable=import-error",
      "--disable=logging-not-lazy",
      "--disable=consider-using-f-string",
      "--disable=line-too-long",
      "--disable=invalid-name",
      "--disable=global-statement",
      "--disable=broad-exception-raised",
      "--disable=broad-exception-caught",
      "--disable=missing-function-docstring",
      "--disable=broad-except",
      "--disable=global-variable-not-assigned",
      "--disable=bare-except",
      "--disable=too-many-arguments",
      "--disable=unused-argument",
      "--disable=import-outside-toplevel",
      "--extension-pkg-whitelist=lxml",
      "--disable=fixme",
      "--disable=too-many-positional-arguments",
      "--disable=too-many-locals",
      "--disable=missing-class-docstring",
      "--disable=logging-fstring-interpolation",
      "--disable=wrong-import-position"
    ]
  }
  ```
