# API Contract

Base path: `/v1`

- `GET /healthz`
- `GET /traces?from=&to=&env=&service=&limit=`
- `GET /traces/{traceId}`
- `GET /dependency?from=&to=&env=`
- `GET /hosts?from=&to=&env=`
- `GET /compare?from=&to=&env=&service=&base=&cand=`

Time format: RFC3339 UTC.
