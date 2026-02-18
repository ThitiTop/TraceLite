# Log Contract

Required fields per event:

- `timestamp` ISO8601 UTC
- `service`
- `env`
- `host`
- `correlationId` (mapped to `trace_id`)
- `event` (`start|end|log`)

Recommended fields:

- `spanId`, `parentSpanId`
- `route`, `method`, `statusCode`, `durationMs`
- `version`
- `attrs` map

Sample NDJSON line:

```json
{"timestamp":"2026-02-18T08:10:11.123Z","service":"checkout","env":"prod","host":"vm-01","level":"INFO","message":"start","correlationId":"a1b2","spanId":"s1","parentSpanId":"","event":"start","route":"POST /orders","method":"POST","statusCode":0,"durationMs":0,"version":"1.12.0","attrs":{"region":"us-east-1"}}
```
