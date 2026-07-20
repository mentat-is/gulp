# Enterprise deployment behind nginx

gULP is an HTTP application server. In production, terminate public HTTPS and
WSS at nginx, an ingress controller, or a managed load balancer. Keep every
gULP backend on a private network; port 8080 must not be publicly reachable.

The supplied [nginx configuration](examples/nginx.conf) is a copy-paste starting
point with these public names:

- `https://gulp.example.com` for the web UI
- `https://api.gulp.example.com` for the REST and WebSocket API

It redirects HTTP to HTTPS, enables TLS 1.2/1.3, forwards client information,
supports WebSocket upgrades and long-lived connections, and streams large
request bodies instead of buffering them to disk. Replace the example IP
addresses and certificate paths before enabling it. The UI upstream example
uses the packaged UI listener on port 3000; use port 80 when nginx connects
directly to the Docker UI container.

## HTTPS UI and API

Serve both public origins over HTTPS. The `gulp.example.com` server block
terminates TLS for the UI, while `api.gulp.example.com` terminates TLS for REST
and WebSocket traffic. On the UI login page, set the backend address to
`https://api.gulp.example.com`; using an HTTP backend from the HTTPS UI would be
blocked by browsers as mixed content. WebSocket connections then use WSS
through the API server block.

## Backend network and forwarded headers

Bind one or more gULP instances to private addresses on HTTP port 8080 and list
them in the `gulp_api` upstream. Only nginx should be able to connect to those
addresses. Configure Uvicorn's proxy trust with the nginx address or CIDR:

```bash
FORWARDED_ALLOW_IPS=10.0.0.10 GULP_BIND_TO_ADDR=10.0.0.21 gulp
```

`FORWARDED_ALLOW_IPS=*` is safe only on a network where nginx is the sole
backend caller. Never use `*` when clients can reach port 8080 directly,
because they could forge forwarded client addresses and schemes.

Add or remove private backend servers in the `gulp_api` upstream to scale gULP.
The REST and WebSocket API shapes do not change.

The example accepts request bodies up to 10 GiB. Set `client_max_body_size` to
the largest ingestion artifact your deployment intends to accept. Keep
`proxy_request_buffering off` for request streaming.

## Prometheus

When Prometheus metrics are enabled, configure Prometheus to scrape every gULP
instance directly on its private address, for example
`http://10.0.0.21:8080/metrics` and `http://10.0.0.22:8080/metrics`. Do not scrape
the load-balanced public API: each request could reach a different instance,
and the metrics endpoint should not be internet-accessible. The nginx example
returns 404 for the default `/metrics` path. If `prometheus_endpoint` is
changed, block that custom path as well. Keep Prometheus and Grafana private;
see the [observability guide](observability.md) for backend configuration.

## Optional mutual TLS

Deployments that previously required gULP client certificates can enforce them
at nginx. Uncomment `ssl_client_certificate`, `ssl_verify_client`, and
`ssl_verify_depth` in the API server block, then point the CA directive at the
issuer trusted for client certificates. nginx rejects untrusted clients before
their requests reach gULP.

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
