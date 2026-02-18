package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"trace-lite/api/internal/clickhouse"
)

type Handler struct {
	ch *clickhouse.Client
}

var safeToken = regexp.MustCompile(`^[a-zA-Z0-9._:/-]+$`)

func New(ch *clickhouse.Client) *Handler {
	return &Handler{ch: ch}
}

func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.ch.Ping(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
}

func (h *Handler) Traces(w http.ResponseWriter, r *http.Request) {
	from, to := parseRange(r)
	limit := parseLimit(r, 200)
	env := sanitize(r.URL.Query().Get("env"))
	service := sanitize(r.URL.Query().Get("service"))

	where := []string{
		fmt.Sprintf("start_ts >= toDateTime64('%s', 3, 'UTC')", chTime(from)),
		fmt.Sprintf("start_ts < toDateTime64('%s', 3, 'UTC')", chTime(to)),
	}
	if env != "" {
		where = append(where, fmt.Sprintf("env = '%s'", env))
	}
	if service != "" {
		where = append(where, fmt.Sprintf("root_service = '%s'", service))
	}

	sql := fmt.Sprintf(`
SELECT trace_id, env, root_service, start_ts, end_ts, duration_ms, span_count, service_count, error_count, critical_path_ms, versions
FROM traces
WHERE %s
ORDER BY start_ts DESC
LIMIT %d`, strings.Join(where, " AND "), limit)

	d, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": d})
}

func (h *Handler) TraceByID(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/v1/traces/")
	id = sanitize(id)
	if id == "" {
		http.Error(w, "invalid trace id", http.StatusBadRequest)
		return
	}

	traceSQL := fmt.Sprintf(`
SELECT trace_id, env, root_service, start_ts, end_ts, duration_ms, span_count, service_count, error_count, critical_path_ms, versions
FROM traces
WHERE trace_id = '%s'
ORDER BY updated_at DESC
LIMIT 1`, id)
	traceRows, err := h.ch.Query(r.Context(), traceSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	spanSQL := fmt.Sprintf(`
SELECT trace_id, span_id, parent_span_id, service, env, host, version, operation, start_ts, end_ts, duration_ms, self_time_ms, status_code, is_error, source
FROM spans
WHERE trace_id = '%s'
ORDER BY start_ts ASC`, id)
	spanRows, err := h.ch.Query(r.Context(), spanSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"trace": firstOrNil(traceRows), "spans": spanRows})
}

func (h *Handler) Dependency(w http.ResponseWriter, r *http.Request) {
	from, to := parseRange(r)
	env := sanitize(r.URL.Query().Get("env"))
	where := []string{
		fmt.Sprintf("bucket_ts >= toDateTime('%s', 'UTC')", chMinute(from)),
		fmt.Sprintf("bucket_ts < toDateTime('%s', 'UTC')", chMinute(to)),
	}
	if env != "" {
		where = append(where, fmt.Sprintf("env = '%s'", env))
	}

	sql := fmt.Sprintf(`
SELECT
  caller_service,
  callee_service,
  sum(calls) AS calls,
  sum(error_calls) AS error_calls,
  round(avg(p50_ms), 2) AS p50_ms,
  round(avg(p95_ms), 2) AS p95_ms,
  max(max_ms) AS max_ms
FROM dependency_edges_minute
WHERE %s
GROUP BY caller_service, callee_service
ORDER BY calls DESC
LIMIT 1000`, strings.Join(where, " AND "))

	d, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"edges": d})
}

func (h *Handler) Hosts(w http.ResponseWriter, r *http.Request) {
	from, to := parseRange(r)
	env := sanitize(r.URL.Query().Get("env"))
	where := []string{
		fmt.Sprintf("bucket_ts >= toDateTime('%s', 'UTC')", chMinute(from)),
		fmt.Sprintf("bucket_ts < toDateTime('%s', 'UTC')", chMinute(to)),
	}
	if env != "" {
		where = append(where, fmt.Sprintf("env = '%s'", env))
	}

	sql := fmt.Sprintf(`
SELECT
  host, logs, errors, last_seen, active_services,
  round(if(logs = 0, 0, errors / logs), 4) AS error_rate
FROM
(
  SELECT
    host,
    sum(logs) AS logs,
    sum(errors) AS errors,
    max(last_seen_ts) AS last_seen,
    max(distinct_services) AS active_services
  FROM host_stats_minute
  WHERE %s
  GROUP BY host
)
ORDER BY logs DESC
LIMIT 2000`, strings.Join(where, " AND "))

	d, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"hosts": d})
}

func (h *Handler) Compare(w http.ResponseWriter, r *http.Request) {
	from, to := parseRange(r)
	env := sanitize(r.URL.Query().Get("env"))
	service := sanitize(r.URL.Query().Get("service"))
	base := sanitize(r.URL.Query().Get("base"))
	cand := sanitize(r.URL.Query().Get("cand"))

	if service == "" || base == "" || cand == "" {
		http.Error(w, "service/base/cand are required", http.StatusBadRequest)
		return
	}

	whereCommon := []string{
		fmt.Sprintf("start_ts >= toDateTime64('%s', 3, 'UTC')", chTime(from)),
		fmt.Sprintf("start_ts < toDateTime64('%s', 3, 'UTC')", chTime(to)),
		fmt.Sprintf("service = '%s'", service),
	}
	if env != "" {
		whereCommon = append(whereCommon, fmt.Sprintf("env = '%s'", env))
	}

	metricsSQL := fmt.Sprintf(`
SELECT
  version,
  count() AS spans,
  round(quantile(0.50)(duration_ms), 2) AS p50_ms,
  round(quantile(0.95)(duration_ms), 2) AS p95_ms,
  round(quantile(0.99)(duration_ms), 2) AS p99_ms,
  round(avg(is_error), 4) AS error_rate
FROM spans
WHERE %s AND version IN ('%s', '%s')
GROUP BY version`, strings.Join(whereCommon, " AND "), base, cand)

	deltaSQL := fmt.Sprintf(`
SELECT
  operation,
  round(quantileIf(0.95)(duration_ms, version = '%s'), 2) AS base_p95_ms,
  round(quantileIf(0.95)(duration_ms, version = '%s'), 2) AS cand_p95_ms,
  round(cand_p95_ms - base_p95_ms, 2) AS delta_p95_ms,
  countIf(version = '%s') AS base_calls,
  countIf(version = '%s') AS cand_calls
FROM spans
WHERE %s AND version IN ('%s', '%s')
GROUP BY operation
HAVING base_calls > 0 AND cand_calls > 0
ORDER BY delta_p95_ms DESC
LIMIT 200`, base, cand, base, cand, strings.Join(whereCommon, " AND "), base, cand)

	metrics, err := h.ch.Query(r.Context(), metricsSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	deltas, err := h.ch.Query(r.Context(), deltaSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"metrics": metrics, "operation_diff": deltas})
}

func firstOrNil(v []map[string]any) any {
	if len(v) == 0 {
		return nil
	}
	return v[0]
}

func parseRange(r *http.Request) (time.Time, time.Time) {
	to := time.Now().UTC()
	from := to.Add(-1 * time.Hour)
	if rawTo := r.URL.Query().Get("to"); rawTo != "" {
		if parsed, err := time.Parse(time.RFC3339, rawTo); err == nil {
			to = parsed.UTC()
		}
	}
	if rawFrom := r.URL.Query().Get("from"); rawFrom != "" {
		if parsed, err := time.Parse(time.RFC3339, rawFrom); err == nil {
			from = parsed.UTC()
		}
	}
	if !from.Before(to) {
		from = to.Add(-1 * time.Hour)
	}
	return from, to
}

func parseLimit(r *http.Request, fallback int) int {
	raw := r.URL.Query().Get("limit")
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil || v <= 0 {
		return fallback
	}
	if v > 5000 {
		return 5000
	}
	return v
}

func sanitize(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	if !safeToken.MatchString(v) {
		return ""
	}
	return v
}

func chTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000")
}

func chMinute(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:00")
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
