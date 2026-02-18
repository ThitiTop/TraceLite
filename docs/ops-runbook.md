# Ops Runbook

## Deploy (single node)

1. Set tokens and env values in `deploy/docker-compose.yml`.
2. Run `make up`.
3. Verify:
   - `curl http://localhost:8080/v1/healthz`
   - `curl -k https://localhost:8443/v1/healthz`

## Troubleshooting

- Fluent Bit not shipping:
  - check `docker compose -f deploy/docker-compose.yml logs fluent-bit`
  - verify `LOG_SHIP_TOKEN` matches collector `INGEST_TOKEN`
- Collector rejecting logs:
  - inspect response body for `rejected` lines
  - validate JSON includes `correlationId`
- UI empty:
  - check API query range and `env/service` filters

## Retention

- `raw_logs`: 30 days
- `spans`: 90 days
- `traces`: 180 days
- `dependency_edges_minute`: 365 days
