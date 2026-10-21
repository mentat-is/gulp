# Server installation

This guide installs a gULP release by hand on one Linux server:

- the backend and the web UI run as systemd services under a dedicated `gulp`
  user;
- OpenSearch, PostgreSQL, Redis and SeaweedFS run in Docker on the same host,
  from the repository's Compose file, each with its own random password.

Commands are for Debian and Ubuntu; on other distributions install the
equivalent packages.

When you are done:

| What | Where |
| --- | --- |
| source checkout | `/opt/gulp/src` |
| Python virtual environment, with gULP and the built web UI | `/opt/gulp/venv` |
| working directory (`GULP_WORKING_DIR`): `gulp_cfg.json`, extra plugins and mappings | `/var/lib/gulp` |
| backend environment: connection URLs and passwords | `/etc/gulp/gulp.env` |
| services: Compose file, passwords, local overrides | `/etc/gulp/services/` |
| backend | `gulp.service`, on `127.0.0.1:8080` |
| web UI | `gulp-ui.service`, on `127.0.0.1:3000` |

Everything listens on `127.0.0.1` only. To reach gULP from other machines, put
nginx in front of it as described in the
[enterprise deployment guide](enterprise_deployment.md).

## 1. Prerequisites

- Linux with systemd; 4 or more CPU cores, at least 8 GB of RAM (16 GB or more
  for real investigations), SSD storage.
- Docker Engine with the Compose plugin, version 2.24 or later: follow
  [Docker's installation guide](https://docs.docker.com/engine/install/), then
  run `sudo systemctl enable --now docker`.
- Python 3.12 or 3.13 (3.13 recommended).
- Node.js 22 and pnpm 10, to build and serve the web UI.

```bash
sudo apt update
sudo apt install -y git curl ca-certificates openssl \
  python3-venv python3-dev \
  build-essential pkg-config rustc cargo \
  libffi-dev libssl-dev libpq-dev zlib1g-dev libxml2-dev libxslt1-dev libmagic1
```

The compilers are for dependencies that have no prebuilt wheel for your Python
version.

Install Node.js 22 and pnpm 10. If your distribution's Node.js is 22 or later
(`apt-cache policy nodejs`), `sudo apt install -y nodejs npm` is enough.
Otherwise, as on Ubuntu 24.04 (Node.js 18), use NodeSource's repository:

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
sudo -u gulp -H git clone --branch v2.0.0 https://github.com/mentat-is/gulp.git /opt/gulp/src
```

Replace `v2.0.0` with the
[latest release](https://github.com/mentat-is/gulp/releases).

## 3. Start the services

Copy the Compose file and its defaults out of the checkout, so that updating
the sources never touches your passwords:

```bash
sudo install -d -m 0700 /etc/gulp/services
sudo cp /opt/gulp/src/docker-compose.yml /opt/gulp/src/.env /etc/gulp/services/
```

Give every service its own random password. Step 5 writes the same values into
the backend's environment, so **run steps 3 to 5 in the same shell**.

```bash
PG_PASSWORD=$(openssl rand -hex 24)
REDIS_PASSWORD=$(openssl rand -hex 24)
S3_SECRET=$(openssl rand -hex 24)
OS_PASSWORD="Gulp-$(openssl rand -hex 16)-A1"

sudo sed -i \
  -e "s|^POSTGRES_PASSWORD=.*|POSTGRES_PASSWORD=${PG_PASSWORD}|" \
  -e "s|^REDIS_PASSWORD=.*|REDIS_PASSWORD=${REDIS_PASSWORD}|" \
  -e "s|^SEAWEEDFS_SECRET_KEY=.*|SEAWEEDFS_SECRET_KEY=${S3_SECRET}|" \
  -e "s|^OPENSEARCH_PASSWORD=.*|OPENSEARCH_PASSWORD=${OS_PASSWORD}|" \
  -e "s|^SFTPD_PASSWORD=.*|SFTPD_PASSWORD=$(openssl rand -hex 24)|" \
  /etc/gulp/services/.env

sudo tee -a /etc/gulp/services/.env >/dev/null <<'EOF'

# server installation
COMPOSE_PROJECT_NAME=gulp
GULP_WORKING_DIR=/var/lib/gulp
# OpenSearch heap: about half of the RAM left after 4 GB for everything else,
# at most 31g (4g on a 12 GB server, 6g on 16 GB, 14g on 32 GB)
OPENSEARCH_JAVA_OPTS="-Xms4g -Xmx4g"
EOF
```

Add an override file with the settings a server needs. Docker Compose reads it
automatically next to `docker-compose.yml`:

```bash
sudo tee /etc/gulp/services/docker-compose.override.yml >/dev/null <<'EOF'
# Server settings: every port on 127.0.0.1 only, OpenSearch heap and memory
# locking, more PostgreSQL connections and shared memory.
services:
  os01:
    environment:
      OPENSEARCH_JAVA_OPTS: ${OPENSEARCH_JAVA_OPTS}
    ulimits:
      memlock: { soft: -1, hard: -1 }
      nofile: { soft: 65536, hard: 65536 }
    ports: !override
      - "127.0.0.1:9200:9200"
  postgres:
    command: ["postgres", "-c", "max_connections=${POSTGRES_MAX_CONNECTIONS}"]
    shm_size: 1g
    ports: !override
      - "127.0.0.1:5432:5432"
  redis:
    ports: !override
      - "127.0.0.1:6379:6379"
  seaweedfs:
    ports: !override
      - "127.0.0.1:9000:9000"
      - "127.0.0.1:9001:8888"
EOF

sudo docker compose --project-directory /etc/gulp/services up -d
```

> **Keep the `127.0.0.1` bindings.** The supplied Compose file disables
> OpenSearch's security plugin, so OpenSearch accepts requests without
> authentication: listening on the loopback interface is what keeps it private.

The containers restart with Docker at boot. Check that all four are running
and that OpenSearch answers; the first start can take a minute:

```bash
sudo docker compose --project-directory /etc/gulp/services ps
curl -s http://127.0.0.1:9200
```

## 4. Install gULP

Build the web UI into the sources, then install gULP into its own virtual
environment. The package then contains the UI, served by `gulp-webui`.

```bash
sudo -u gulp -H sh -c 'cd /opt/gulp/src && bash scripts/build_webui_package.sh'
sudo -u gulp -H python3 -m venv /opt/gulp/venv
sudo -u gulp -H /opt/gulp/venv/bin/pip install --upgrade pip
sudo -u gulp -H /opt/gulp/venv/bin/pip install /opt/gulp/src
```

If `python3` is 3.14 or newer and a dependency fails to build, create the
virtual environment with Python 3.13 instead.

## 5. Configure the backend

Write the backend's environment, in the same shell as step 3:

```bash
sudo tee /etc/gulp/gulp.env >/dev/null <<EOF
GULP_WORKING_DIR=/var/lib/gulp
GULP_BIND_TO_ADDR=127.0.0.1
GULP_BIND_TO_PORT=8080
GULP_POSTGRES_URL=postgresql+psycopg://postgres:${PG_PASSWORD}@127.0.0.1:5432/gulp
GULP_OPENSEARCH_URL=http://admin:${OS_PASSWORD}@127.0.0.1:9200
GULP_REDIS_URL=redis://:${REDIS_PASSWORD}@127.0.0.1:6379/0
GULP_S3_URL=http://admin:${S3_SECRET}@127.0.0.1:9000
PYTHONUNBUFFERED=1
EOF
sudo chown root:gulp /etc/gulp/gulp.env
sudo chmod 0640 /etc/gulp/gulp.env
```

These variables take precedence over the URLs in `gulp_cfg.json`. Every other
setting lives in `/var/lib/gulp/gulp_cfg.json`, which the first start creates
from the template; restart the backend after changing it.

## 6. Run the backend and the web UI

```bash
sudo tee /etc/systemd/system/gulp.service >/dev/null <<'EOF'
[Unit]
Description=gULP backend
Wants=network-online.target docker.service
After=network-online.target docker.service

[Service]
Type=simple
User=gulp
Group=gulp
EnvironmentFile=/etc/gulp/gulp.env
Environment=PATH=/opt/gulp/venv/bin:/usr/local/bin:/usr/bin:/bin
WorkingDirectory=/var/lib/gulp
ExecStart=/opt/gulp/venv/bin/gulp
KillSignal=SIGINT
KillMode=mixed
TimeoutStopSec=60
Restart=always
RestartSec=10
LimitNOFILE=65535
UMask=0027
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF

sudo tee /etc/systemd/system/gulp-ui.service >/dev/null <<'EOF'
[Unit]
Description=gULP web UI
Wants=network-online.target
After=network-online.target gulp.service

[Service]
Type=simple
User=gulp
Group=gulp
Environment=PATH=/opt/gulp/venv/bin:/usr/local/bin:/usr/bin:/bin
ExecStart=/opt/gulp/venv/bin/gulp-webui --host 127.0.0.1 --port 3000
Restart=always
RestartSec=5
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=full
ProtectHome=true

[Install]
WantedBy=multi-user.target
EOF

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

## Next steps

- **HTTPS and access from the network:** follow the
  [enterprise deployment guide](enterprise_deployment.md). With nginx on the
  same host, add `FORWARDED_ALLOW_IPS=127.0.0.1` to `/etc/gulp/gulp.env` and
  restart the backend.
- **Extra plugins and mapping files:** copy them into `/var/lib/gulp/plugins`
  and `/var/lib/gulp/mapping_files`, then run `sudo systemctl restart gulp`.
- **Sizing and monitoring:** [scaling](scaling.md) and
  [observability](observability.md).
- **Problems:** [troubleshooting](troubleshooting.md); the backend's log is
  `journalctl -u gulp`.

## Update

Read the [changelog](../CHANGELOG.md) of the new release first: it lists the
database migrations and configuration changes it needs. Compare the new
release's `docker-compose.yml` with your copy in `/etc/gulp/services`. Back up
PostgreSQL, then update:

```bash
sudo docker compose --project-directory /etc/gulp/services exec -T postgres \
  pg_dump -U postgres gulp | gzip > "gulp-$(date +%F).sql.gz"

sudo systemctl stop gulp-ui gulp
sudo -u gulp -H git -C /opt/gulp/src fetch --tags
sudo -u gulp -H git -C /opt/gulp/src checkout --force v2.0.1
sudo -u gulp -H sh -c 'cd /opt/gulp/src && bash scripts/build_webui_package.sh'
sudo -u gulp -H /opt/gulp/venv/bin/pip install /opt/gulp/src
sudo systemctl start gulp gulp-ui
```

Replace `v2.0.1` with the release you are updating to. `--force` discards
the changes the web UI build of some releases makes to tracked files (in
`v2.0.0`, `gulp-ui/public/version`), which would otherwise stop the checkout:
keep no changes of your own in `/opt/gulp/src`.
