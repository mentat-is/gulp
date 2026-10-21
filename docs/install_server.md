# Server installation

This guide installs a gULP release by hand on one Linux server:

- the backend and the web UI run as systemd services under a dedicated `gulp`
  user;
- OpenSearch, PostgreSQL, Redis and SeaweedFS run in Docker on the same host,
  from the repository's Compose file, with their own random passwords.

Commands are for Debian and Ubuntu; on other distributions install the
equivalent packages. The guide needs a gULP release newer than `v2.0.0`.

When you are done:

| What | Where |
| --- | --- |
| source checkout | `/opt/gulp/src` |
| Python virtual environment, with gULP and the built web UI | `/opt/gulp/venv` |
| working directory (`GULP_WORKING_DIR`): `gulp_cfg.json`, extra plugins and mappings | `/var/lib/gulp` |
| services: Compose file and their passwords (`.env`) | `/etc/gulp/services/` |
| backend environment: connection URLs | `/etc/gulp/gulp.env` |
| backend | `gulp.service`, on `127.0.0.1:8080` |
| web UI | `gulp-ui.service`, on `127.0.0.1:3000` |

Everything listens on `127.0.0.1` only. To reach gULP from other machines,
put nginx in front of it: [Run nginx in Docker](enterprise_deployment.md#run-nginx-in-docker)
takes five commands.

## In short

1. Install Docker, Python with a C compiler, Node.js 22 and pnpm 10, and raise
   `vm.max_map_count` to 262144.
2. Create the `gulp` user and clone a release into `/opt/gulp/src`.
3. Copy the Compose file and `.env` to `/etc/gulp/services`, set random
   passwords and the OpenSearch heap, start the services.
4. As `gulp`: build the web UI into the sources, create the virtual
   environment, `pip install` the sources.
5. Write `/etc/gulp/gulp.env` from the services' passwords.
6. Install the systemd units from
   [docs/examples/systemd](examples/systemd/) and start them.
7. Log in through an SSH tunnel and change the admin password.
8. To reach gULP from other machines, run nginx in Docker in front of it.

## 1. Prerequisites

- Linux with systemd; 4 or more CPU cores, at least 8 GB of RAM (16 GB or more
  for real investigations), SSD storage.
- Docker Engine with the Compose plugin: follow
  [Docker's installation guide](https://docs.docker.com/engine/install/), then
  run `sudo systemctl enable --now docker`.
- Python 3.12 or 3.13 (3.13 recommended), with a C compiler for the few
  dependencies that have no prebuilt wheel.
- Node.js 22 and pnpm 10, to build and serve the web UI.

```bash
sudo apt update
sudo apt install -y git curl ca-certificates openssl python3-venv python3-dev build-essential
```

If your distribution's Node.js is 22 or later (`apt-cache policy nodejs`),
`sudo apt install -y nodejs npm` is enough. Otherwise, as on Ubuntu 24.04
(Node.js 18), use NodeSource's repository. Then install pnpm:

```bash
curl -fsSL https://deb.nodesource.com/setup_22.x -o nodesource_setup.sh
sudo bash nodesource_setup.sh
sudo apt install -y nodejs
sudo npm install -g pnpm@10
```

OpenSearch needs a higher memory-map limit:

```bash
echo 'vm.max_map_count=262144' | sudo tee /etc/sysctl.d/99-gulp-opensearch.conf
sudo sysctl --system
```

## 2. Create the user and get the sources

```bash
sudo useradd --system --create-home --home-dir /opt/gulp --shell /usr/sbin/nologin gulp
sudo install -d -o gulp -g gulp -m 0750 /var/lib/gulp
sudo -u gulp -H git clone --branch vX.Y.Z https://github.com/mentat-is/gulp.git /opt/gulp/src
```

Replace `vX.Y.Z` with the
[latest release](https://github.com/mentat-is/gulp/releases).

## 3. Start the services

Copy the Compose file and its defaults out of the checkout, so that updating
the sources never touches your passwords. Then give each service a random
password, and size the OpenSearch heap:

```bash
sudo install -d -m 0700 /etc/gulp/services
sudo cp /opt/gulp/src/docker-compose.yml /opt/gulp/src/.env /etc/gulp/services/
sudo sed -i \
  -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=$(openssl rand -hex 24)|" \
  -e "s|^REDIS_PASSWORD=.*|REDIS_PASSWORD=$(openssl rand -hex 24)|" \
  -e "s|^SEAWEEDFS_SECRET_KEY=.*|SEAWEEDFS_SECRET_KEY=$(openssl rand -hex 24)|" \
  /etc/gulp/services/.env
sudo tee -a /etc/gulp/services/.env >/dev/null <<'EOF'

# server installation
COMPOSE_PROJECT_NAME=gulp
GULP_WORKING_DIR=/var/lib/gulp
# OpenSearch heap: about half of the RAM left after 4 GB for everything else,
# at most 31g (4g on a 12 GB server, 6g on 16 GB, 14g on 32 GB)
OPENSEARCH_JAVA_OPTS="-Xms4g -Xmx4g"
EOF
sudo docker compose --project-directory /etc/gulp/services up -d
```

The passwords stay in `/etc/gulp/services/.env`: step 5 reads them from there.
`OPENSEARCH_PASSWORD` is not used, because OpenSearch runs without its
security plugin, and `SFTPD_PASSWORD` only matters for the optional `extra`
profile.

> **Keep the services on `127.0.0.1`.** The Compose file binds them to
> `GULP_SERVICES_BIND`, `127.0.0.1` by default. OpenSearch accepts requests
> without authentication: listening on the loopback interface is what keeps it
> private.

The containers restart with Docker at boot. Check that all four are running
and that OpenSearch answers; the first start can take a minute:

```bash
sudo docker compose --project-directory /etc/gulp/services ps
curl -s http://127.0.0.1:9200
```

## 4. Install gULP

As the `gulp` user, build the web UI into the sources, create the virtual
environment and install gULP into it. The package then contains the UI,
served by `gulp-webui`.

```bash
sudo -u gulp -H bash -ec '
  cd /opt/gulp/src
  bash scripts/build_webui_package.sh
  python3 -m venv /opt/gulp/venv
  /opt/gulp/venv/bin/pip install --upgrade pip
  /opt/gulp/venv/bin/pip install .
'
```

If `python3` is 3.14 or newer and a dependency fails to build, create the
virtual environment with Python 3.13 instead.

## 5. Configure the backend

Write the backend's environment from the services' passwords:

```bash
sudo sh -ec '
  . /etc/gulp/services/.env
  umask 027
  cat > /etc/gulp/gulp.env <<EOF
GULP_WORKING_DIR=/var/lib/gulp
GULP_BIND_TO_ADDR=127.0.0.1
GULP_BIND_TO_PORT=8080
GULP_POSTGRES_URL=postgresql+psycopg://${POSTGRES_USER}:${POSTGRES_PASSWORD}@127.0.0.1:5432/${POSTGRES_DB}
GULP_OPENSEARCH_URL=http://127.0.0.1:9200
GULP_REDIS_URL=redis://:${REDIS_PASSWORD}@127.0.0.1:6379/0
GULP_S3_URL=http://${SEAWEEDFS_ACCESS_KEY}:${SEAWEEDFS_SECRET_KEY}@127.0.0.1:9000
PYTHONUNBUFFERED=1
EOF
  chown root:gulp /etc/gulp/gulp.env
'
```

These variables take precedence over the URLs in `gulp_cfg.json`. Every other
setting lives in `/var/lib/gulp/gulp_cfg.json`, which the first start creates
from the template; restart the backend after changing it.

## 6. Run the backend and the web UI

The systemd units are in [docs/examples/systemd](examples/systemd/); adapt
their paths if yours differ.

```bash
sudo cp /opt/gulp/src/docs/examples/systemd/gulp.service \
  /opt/gulp/src/docs/examples/systemd/gulp-ui.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now gulp gulp-ui
sudo journalctl -u gulp -f
```

Wait for `Application startup complete`, then press Ctrl+C. If a service is
not ready yet, `gulp` stops with an error that names it, and systemd starts it
again 10 seconds later. On its first start, `gulp-webui` downloads the static
file server it uses (`serve`) through pnpm.

> **The first start initializes gULP.** In an empty working directory, gULP
> creates `gulp_cfg.json`, recreates the collaboration database and creates the
> `test_operation` operation. Never point a new working directory at a
> database that already holds data.

## 7. Log in

Both services listen on the server's loopback interface. From your
workstation, open an SSH tunnel to them:

```bash
ssh -L 3000:127.0.0.1:3000 -L 8080:127.0.0.1:8080 you@your-server
```

Open <http://localhost:3000>, enter `http://localhost:8080` as the backend
address and log in as `admin` / `admin`. **Change the admin password right
away**, from the web UI's user management or with the `user_update` API.

## If something goes wrong

- **OpenSearch keeps restarting:** read
  `sudo docker compose --project-directory /etc/gulp/services logs os01`.
  Usually `vm.max_map_count` is too low (step 1) or the heap is larger than
  the memory left.
- **`gulp` keeps restarting:** `journalctl -u gulp` names the service it
  cannot reach. Check the containers with `docker compose ps` (step 3) and the
  URLs in `/etc/gulp/gulp.env`.
- **The login page opens but the login fails:** the backend address is the one
  your browser can reach: `http://localhost:8080` through the tunnel, or the
  API name behind nginx.
- Anything else: [troubleshooting](troubleshooting.md).

## Next steps

- **HTTPS and access from the network:** run nginx in Docker in front of
  gULP, as described in
  [Run nginx in Docker](enterprise_deployment.md#run-nginx-in-docker). It
  publishes the web UI and the API on one name, `https://<your name>` with the
  backend address `https://<your name>/api`.
- **Extra plugins and mapping files:** copy them into `/var/lib/gulp/plugins`
  and `/var/lib/gulp/mapping_files`, then run `sudo systemctl restart gulp`.
- **Sizing and monitoring:** [scaling](scaling.md) and
  [observability](observability.md).

## Update

Read the [changelog](../CHANGELOG.md) of the new release first: it lists the
database migrations and configuration changes it needs. Compare the new
release's `docker-compose.yml` and `docs/examples/systemd` with your copies.
Back up PostgreSQL, then update:

```bash
sudo docker compose --project-directory /etc/gulp/services exec -T postgres \
  pg_dump -U postgres gulp | gzip > "gulp-$(date +%F).sql.gz"

sudo systemctl stop gulp-ui gulp
sudo -u gulp -H bash -ec '
  cd /opt/gulp/src
  git fetch --tags
  git checkout --force vX.Y.Z
  bash scripts/build_webui_package.sh
  /opt/gulp/venv/bin/pip install .
'
sudo systemctl start gulp gulp-ui
```

Replace `vX.Y.Z` with the release you are updating to. `--force` discards
the changes the web UI build of some releases makes to tracked files (in
`v2.0.0`, `gulp-ui/public/version`), which would otherwise stop the checkout:
keep no changes of your own in `/opt/gulp/src`.
