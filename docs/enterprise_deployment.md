# Enterprise deployment behind nginx

gULP is an HTTP application server. In production, terminate public HTTPS and
WSS at nginx, an ingress controller, or a managed load balancer. Keep every
gULP backend on a private network; port 8080 must not be publicly reachable.

The supplied [nginx configuration](examples/nginx/gulp.conf) publishes gULP on
one name, `https://gulp.example.com`:

- `/`: the web UI;
- `/api/`: the REST API;
- `/api/ws`, `/api/ws_ingest_raw`, `/api/ws_client_data`: the WebSockets.

One name needs one certificate, and the browser talks to a single origin. The
configuration redirects HTTP to HTTPS, enables TLS 1.2/1.3, forwards client
information, supports WebSocket upgrades and long-lived connections, streams
large request bodies instead of buffering them to disk, lets browsers cache the
web UI's static files, and compresses JSON answers.

## Run nginx in Docker

Run nginx from the official image, with the supplied
[Compose file](examples/nginx/docker-compose.yml). It uses the host's network:
nginx listens on ports 80 and 443 and reaches gULP on `127.0.0.1`, as installed
by the [server installation](install_server.md).

```bash
sudo install -d -m 0750 /etc/gulp/nginx/certs
sudo cp /opt/gulp/src/docs/examples/nginx/docker-compose.yml \
  /opt/gulp/src/docs/examples/nginx/gulp.conf /etc/gulp/nginx/
sudo sed -i 's/gulp\.example\.com/gulp.your-domain.com/g' /etc/gulp/nginx/gulp.conf
sudo cp fullchain.pem privkey.pem /etc/gulp/nginx/certs/
sudo docker compose --project-directory /etc/gulp/nginx up -d
```

Use your public name instead of `gulp.your-domain.com`, and your certificate
chain and key for `fullchain.pem` and `privkey.pem`. To try it with a
self-signed certificate instead:

```bash
sudo openssl req -x509 -newkey rsa:2048 -nodes -days 365 -subj "/CN=gulp.your-domain.com" \
  -keyout /etc/gulp/nginx/certs/privkey.pem -out /etc/gulp/nginx/certs/fullchain.pem
```

Then tell gULP to trust nginx's forwarded headers (see below) by adding
`FORWARDED_ALLOW_IPS=127.0.0.1` to `/etc/gulp/gulp.env` and running
`sudo systemctl restart gulp`. Open `https://gulp.your-domain.com` and enter
`https://gulp.your-domain.com/api` as the backend address on the login page.

After changing `gulp.conf`, check and apply it with
`sudo docker exec gulp-nginx nginx -t` and
`sudo docker exec gulp-nginx nginx -s reload`. With a distribution's nginx
instead of the container, put `gulp.conf` in `/etc/nginx/conf.d/` and adapt the
certificate paths.

## Backend network and forwarded headers

Bind one or more gULP instances to private addresses on HTTP port 8080 and list
them in the `gulp_api` upstream; the example has one, on `127.0.0.1`. Only
nginx should be able to connect to those addresses. Configure Uvicorn's proxy
trust with the nginx address or CIDR:

```bash
FORWARDED_ALLOW_IPS=10.0.0.10 GULP_BIND_TO_ADDR=10.0.0.21 gulp
```

`FORWARDED_ALLOW_IPS=*` is safe only on a network where nginx is the sole
backend caller. Never use `*` when clients can reach port 8080 directly,
because they could forge forwarded client addresses and schemes.

Add or remove private backend servers in the `gulp_api` upstream to scale gULP.
The REST and WebSocket API shapes do not change. The `gulp_ui` upstream points
to the web UI served by `gulp-webui` on port 3000; use port 80 when nginx
connects to the Docker UI container instead.

The example accepts request bodies of any size (`client_max_body_size 0`). Set
`client_max_body_size` to the largest ingestion artifact your deployment
intends to accept. Keep `proxy_request_buffering off` for request streaming.

## Prometheus

When Prometheus metrics are enabled, configure Prometheus to scrape every gULP
instance directly on its private address, for example
`http://10.0.0.21:8080/metrics` and `http://10.0.0.22:8080/metrics`. Do not scrape
the load-balanced public API: each request could reach a different instance,
and the metrics endpoint should not be internet-accessible. The nginx example
returns 404 for the default path, `/api/metrics`. If `prometheus_endpoint` is
changed, block that custom path as well. Keep Prometheus and Grafana private;
see the [observability guide](observability.md) for backend configuration.

## Optional mutual TLS

Deployments that previously required gULP client certificates can enforce them
at nginx. Uncomment `ssl_client_certificate`, `ssl_verify_client`, and
`ssl_verify_depth` in the HTTPS server block, and put the CA trusted for client
certificates in the certificate folder as `client-ca.pem`. nginx rejects
untrusted clients before their requests reach gULP.

## Outbound service TLS

gULP should connect directly to its data services; do not route PostgreSQL,
OpenSearch, Redis, or S3 through the public nginx edge. Use each native client:

- PostgreSQL: put `sslmode=verify-full`, `sslrootcert`, `sslcert`, `sslkey`, and
  `sslpassword` as needed in `postgres_url` or `GULP_POSTGRES_URL`.
- OpenSearch: use the complete `https://` URL in `opensearch_url` or
  `GULP_OPENSEARCH_URL`. Use `SSL_CERT_FILE` or `SSL_CERT_DIR` for a private CA.
- Redis: use a `rediss://` URL in `redis_url` or `GULP_REDIS_URL`, including
  redis-py TLS query parameters such as `ssl_cert_reqs` and `ssl_ca_certs`.
- S3/SeaweedFS: use an `https://` endpoint in `s3_url` or `GULP_S3_URL`. Use
  `AWS_CA_BUNDLE` for a private CA.

Native HTTPS certificate verification remains enabled. Install the correct CA
instead of disabling verification.
