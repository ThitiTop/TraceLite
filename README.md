# trace-lite

Open-source log intelligence starter for microservices using agent shipping:

`Service -> Fluent Bit -> HTTPS Collector (Go) -> ClickHouse -> REST API -> React UI`

## Quick start

1. `docker compose -f deploy/docker-compose.yml up --build`
2. Send logs to `sample-logs/app.log` (or bind real app logs)
3. Open UI: `http://localhost:3000`
4. API health: `http://localhost:8080/v1/healthz`

## Components

- `collector`: HTTPS ingest + span/trace reconstruction
- `api`: Query endpoints for traces, hosts, dependency graph, compare
- `ui`: React dashboard with React Flow dependency graph
- `deploy/clickhouse/init/001_schema.sql`: ClickHouse schema
- `deploy/fluent-bit/fluent-bit.conf`: Fluent Bit outbound-only shipping config

## Notes

- Collector supports auto self-signed TLS for local compose.
- Production should use real certs + token/mTLS.
