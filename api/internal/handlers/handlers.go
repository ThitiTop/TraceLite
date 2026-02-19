package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"trace-lite/api/internal/clickhouse"
)

type Handler struct {
	ch *clickhouse.Client
}

var safeToken = regexp.MustCompile(`^[a-zA-Z0-9._:/-]+$`)

type traceSpan struct {
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Service       string
	Env           string
	Host          string
	Version       string
	Operation     string
	StartTS       string
	EndTS         string
	StartTime     time.Time
	EndTime       time.Time
	DurationMs    uint32
	SelfTimeMs    uint32
	StatusCode    uint16
	IsError       bool
	Source        string
	Depth         int
	WaitMs        uint32
	BlockingRatio float64
	Children      []*traceSpan
	IsCritical    bool
	Explanation   string
	LeftPct       float64
	WidthPct      float64
}

type rootCauseRank struct {
	Service         string  `json:"service"`
	Score           float64 `json:"score"`
	LatencyDeltaPct float64 `json:"latency_delta_pct"`
	ErrorDeltaPct   float64 `json:"error_delta_pct"`
	CallDeltaPct    float64 `json:"call_delta_pct"`
	BlockingRatio   float64 `json:"blocking_ratio"`
	Reason          string  `json:"reason"`
}

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
	tail := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/traces/"), "/")
	if tail == "" {
		http.Error(w, "invalid trace id", http.StatusBadRequest)
		return
	}
	parts := strings.Split(tail, "/")
	id := sanitize(parts[0])
	if id == "" {
		http.Error(w, "invalid trace id", http.StatusBadRequest)
		return
	}
	mode := ""
	if len(parts) > 1 {
		mode = strings.ToLower(strings.TrimSpace(parts[1]))
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

	if mode == "waterfall" || mode == "drilldown" {
		drill := buildTraceDrilldown(spanRows)
		writeJSON(w, http.StatusOK, map[string]any{
			"trace":         firstOrNil(traceRows),
			"waterfall":     drill["waterfall"],
			"critical_path": drill["critical_path"],
			"error_chains":  drill["error_chains"],
			"slow_spots":    drill["slow_spots"],
			"trace_window":  drill["trace_window"],
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"trace": firstOrNil(traceRows), "spans": spanRows})
}

func (h *Handler) Dependency(w http.ResponseWriter, r *http.Request) {
	if strings.HasSuffix(r.URL.Path, "/diff") {
		h.DependencyDiff(w, r)
		return
	}

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
  caller_service, callee_service, calls, error_calls, avg_latency_ms, p95_ms, max_ms,
  round(if(calls = 0, 0, error_calls / calls), 4) AS error_rate
FROM (
  SELECT
    caller_service,
    callee_service,
    sum(calls) AS calls,
    sum(error_calls) AS error_calls,
    round(avg((p50_ms + p95_ms)/2), 2) AS avg_latency_ms,
    round(avg(p95_ms), 2) AS p95_ms,
    max(max_ms) AS max_ms
  FROM dependency_edges_minute
  WHERE %s
  GROUP BY caller_service, callee_service
)
ORDER BY calls DESC
LIMIT 1000`, strings.Join(where, " AND "))

	d, err := h.ch.Query(r.Context(), sql)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"edges": d})
}

func (h *Handler) DependencyDiff(w http.ResponseWriter, r *http.Request) {
	from, to := parseRange(r)
	env := sanitize(r.URL.Query().Get("env"))
	service := sanitize(r.URL.Query().Get("service"))
	base := sanitize(r.URL.Query().Get("base"))
	cand := sanitize(r.URL.Query().Get("cand"))
	if base == "" || cand == "" {
		http.Error(w, "base/cand are required", http.StatusBadRequest)
		return
	}

	commonWhere := []string{
		fmt.Sprintf("bucket_ts >= toDateTime('%s', 'UTC')", chMinute(from)),
		fmt.Sprintf("bucket_ts < toDateTime('%s', 'UTC')", chMinute(to)),
	}
	if env != "" {
		commonWhere = append(commonWhere, fmt.Sprintf("env = '%s'", env))
	}
	if service != "" {
		commonWhere = append(commonWhere, fmt.Sprintf("(caller_service = '%s' OR callee_service = '%s')", service, service))
	}

	edgeSQL := func(version string) string {
		where := append([]string{}, commonWhere...)
		where = append(where, fmt.Sprintf("(caller_version = '%s' OR callee_version = '%s')", version, version))
		return fmt.Sprintf(`
SELECT caller_service, callee_service, calls, p95_ms,
       round(if(calls = 0, 0, error_calls / calls), 4) AS error_rate
FROM (
  SELECT caller_service, callee_service,
         sum(calls) AS calls,
         sum(error_calls) AS error_calls,
         round(avg(p95_ms), 2) AS p95_ms
  FROM dependency_edges_minute
  WHERE %s
  GROUP BY caller_service, callee_service
)`, strings.Join(where, " AND "))
	}

	baseRows, err := h.ch.Query(r.Context(), edgeSQL(base))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	candRows, err := h.ch.Query(r.Context(), edgeSQL(cand))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	type edgeStats struct {
		Calls     float64
		P95       float64
		ErrorRate float64
	}
	baseMap := map[string]edgeStats{}
	candMap := map[string]edgeStats{}

	for _, row := range baseRows {
		k := fmt.Sprintf("%s->%s", toString(row["caller_service"]), toString(row["callee_service"]))
		baseMap[k] = edgeStats{Calls: toFloat(row["calls"]), P95: toFloat(row["p95_ms"]), ErrorRate: toFloat(row["error_rate"])}
	}
	for _, row := range candRows {
		k := fmt.Sprintf("%s->%s", toString(row["caller_service"]), toString(row["callee_service"]))
		candMap[k] = edgeStats{Calls: toFloat(row["calls"]), P95: toFloat(row["p95_ms"]), ErrorRate: toFloat(row["error_rate"])}
	}

	keys := map[string]struct{}{}
	for k := range baseMap {
		keys[k] = struct{}{}
	}
	for k := range candMap {
		keys[k] = struct{}{}
	}

	edges := make([]map[string]any, 0, len(keys))
	newCount, removedCount, changedCount := 0, 0, 0
	for k := range keys {
		parts := strings.Split(k, "->")
		b, bok := baseMap[k]
		c, cok := candMap[k]
		status := "changed"
		switch {
		case !bok && cok:
			status = "new"
			newCount++
		case bok && !cok:
			status = "removed"
			removedCount++
		default:
			changedCount++
		}

		edges = append(edges, map[string]any{
			"caller_service":        parts[0],
			"callee_service":        parts[1],
			"status":                status,
			"base_calls":            b.Calls,
			"cand_calls":            c.Calls,
			"call_diff":             c.Calls - b.Calls,
			"call_diff_pct":         pctDelta(b.Calls, c.Calls),
			"base_p95_ms":           b.P95,
			"cand_p95_ms":           c.P95,
			"p95_diff_ms":           c.P95 - b.P95,
			"base_error_rate":       b.ErrorRate,
			"cand_error_rate":       c.ErrorRate,
			"error_rate_diff":       c.ErrorRate - b.ErrorRate,
			"is_new_edge":           status == "new",
			"is_removed_edge":       status == "removed",
			"is_high_call_increase": pctDelta(b.Calls, c.Calls) >= 100,
		})
	}

	sort.Slice(edges, func(i, j int) bool {
		return toFloat(edges[i]["call_diff_pct"]) > toFloat(edges[j]["call_diff_pct"])
	})

	writeJSON(w, http.StatusOK, map[string]any{
		"summary": map[string]any{
			"new_edges":     newCount,
			"removed_edges": removedCount,
			"changed_edges": changedCount,
		},
		"edges": edges,
	})
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

	traceWhere := []string{
		fmt.Sprintf("start_ts >= toDateTime64('%s', 3, 'UTC')", chTime(from)),
		fmt.Sprintf("start_ts < toDateTime64('%s', 3, 'UTC')", chTime(to)),
		fmt.Sprintf("root_service = '%s'", service),
	}
	if env != "" {
		traceWhere = append(traceWhere, fmt.Sprintf("env = '%s'", env))
	}
	traceSubquery := fmt.Sprintf("SELECT trace_id FROM traces WHERE %s", strings.Join(traceWhere, " AND "))
	spanWhereAll := fmt.Sprintf("trace_id IN (%s) AND version IN ('%s', '%s')", traceSubquery, base, cand)
	spanWhereService := fmt.Sprintf("%s AND service = '%s'", spanWhereAll, service)

	metricsSQL := fmt.Sprintf(`
SELECT
  version,
  count() AS spans,
  round(quantile(0.50)(duration_ms), 2) AS p50_ms,
  round(quantile(0.95)(duration_ms), 2) AS p95_ms,
  round(quantile(0.99)(duration_ms), 2) AS p99_ms,
  round(avg(is_error), 4) AS error_rate
FROM spans
WHERE %s
GROUP BY version`, spanWhereService)

	deltaSQL := fmt.Sprintf(`
SELECT
  operation,
  round(quantileIf(0.95)(duration_ms, version = '%s'), 2) AS base_p95_ms,
  round(quantileIf(0.95)(duration_ms, version = '%s'), 2) AS cand_p95_ms,
  round(cand_p95_ms - base_p95_ms, 2) AS delta_p95_ms,
  countIf(version = '%s') AS base_calls,
  countIf(version = '%s') AS cand_calls
FROM spans
WHERE %s
GROUP BY operation
HAVING base_calls > 0 AND cand_calls > 0
ORDER BY delta_p95_ms DESC
LIMIT 200`, base, cand, base, cand, spanWhereService)

	rootCauseSQL := fmt.Sprintf(`
SELECT
  service,
  version,
  count() AS calls,
  round(quantile(0.95)(duration_ms), 2) AS p95_ms,
  round(avg(is_error), 4) AS error_rate,
  round(avg(greatest(duration_ms - self_time_ms, 0)), 2) AS wait_ms,
  round(avg(if(duration_ms = 0, 0, greatest(duration_ms - self_time_ms, 0) / duration_ms)), 4) AS blocking_ratio
FROM spans
WHERE %s
GROUP BY service, version`, spanWhereAll)

	summarySQL := fmt.Sprintf(`
SELECT
  round(quantileIf(0.95)(duration_ms, version = '%s'), 2) AS base_p95,
  round(quantileIf(0.95)(duration_ms, version = '%s'), 2) AS cand_p95,
  round(avgIf(is_error, version = '%s'), 4) AS base_error_rate,
  round(avgIf(is_error, version = '%s'), 4) AS cand_error_rate,
  countIf(version = '%s') AS base_calls,
  countIf(version = '%s') AS cand_calls
FROM spans
WHERE %s`, base, cand, base, cand, base, cand, spanWhereService)

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
	rootRows, err := h.ch.Query(r.Context(), rootCauseSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	summaryRows, err := h.ch.Query(r.Context(), summarySQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	rootCauses := buildRootCauseRanking(rootRows, base, cand)
	anomalies := buildAnomalyBadges(summaryRows)

	writeJSON(w, http.StatusOK, map[string]any{
		"metrics":        metrics,
		"operation_diff": deltas,
		"root_causes":    rootCauses,
		"anomalies":      anomalies,
	})
}

func (h *Handler) Errors(w http.ResponseWriter, r *http.Request) {
	from, to := parseRange(r)
	env := sanitize(r.URL.Query().Get("env"))
	service := sanitize(r.URL.Query().Get("service"))
	base := sanitize(r.URL.Query().Get("base"))
	cand := sanitize(r.URL.Query().Get("cand"))

	traceWhere := []string{
		fmt.Sprintf("start_ts >= toDateTime64('%s', 3, 'UTC')", chTime(from)),
		fmt.Sprintf("start_ts < toDateTime64('%s', 3, 'UTC')", chTime(to)),
	}
	if env != "" {
		traceWhere = append(traceWhere, fmt.Sprintf("env = '%s'", env))
	}
	if service != "" {
		traceWhere = append(traceWhere, fmt.Sprintf("root_service = '%s'", service))
	}
	traceSubquery := fmt.Sprintf("SELECT trace_id FROM traces WHERE %s", strings.Join(traceWhere, " AND "))
	spanWhere := fmt.Sprintf("trace_id IN (%s)", traceSubquery)

	serviceBreakdownSQL := fmt.Sprintf(`
SELECT service,
       countIf(is_error = 1) AS errors,
       count() AS calls,
       round(countIf(is_error = 1) / greatest(count(), 1), 4) AS error_rate
FROM spans
WHERE %s
GROUP BY service
ORDER BY errors DESC, calls DESC`, spanWhere)

	topOpsSQL := fmt.Sprintf(`
SELECT service, operation,
       countIf(is_error = 1) AS errors,
       count() AS calls,
       round(countIf(is_error = 1) / greatest(count(), 1), 4) AS error_rate
FROM spans
WHERE %s
GROUP BY service, operation
HAVING errors > 0
ORDER BY errors DESC, error_rate DESC
LIMIT 20`, spanWhere)

	edgeWhere := []string{
		fmt.Sprintf("bucket_ts >= toDateTime('%s', 'UTC')", chMinute(from)),
		fmt.Sprintf("bucket_ts < toDateTime('%s', 'UTC')", chMinute(to)),
	}
	if env != "" {
		edgeWhere = append(edgeWhere, fmt.Sprintf("env = '%s'", env))
	}
	if service != "" {
		edgeWhere = append(edgeWhere, fmt.Sprintf("(caller_service = '%s' OR callee_service = '%s')", service, service))
	}
	propagationSQL := fmt.Sprintf(`
SELECT caller_service, callee_service, error_calls, calls,
       round(if(calls = 0, 0, error_calls / calls), 4) AS error_rate
FROM (
  SELECT caller_service, callee_service,
         sum(error_calls) AS error_calls,
         sum(calls) AS calls
  FROM dependency_edges_minute
  WHERE %s
  GROUP BY caller_service, callee_service
)
WHERE error_calls > 0
ORDER BY error_calls DESC
LIMIT 20`, strings.Join(edgeWhere, " AND "))

	breakdown, err := h.ch.Query(r.Context(), serviceBreakdownSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	topOps, err := h.ch.Query(r.Context(), topOpsSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}
	propagation, err := h.ch.Query(r.Context(), propagationSQL)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadGateway)
		return
	}

	newErrors := []map[string]any{}
	if base != "" && cand != "" {
		newErrSQL := fmt.Sprintf(`
SELECT service, operation,
       countIf(is_error = 1 AND version = '%s') AS base_errors,
       countIf(is_error = 1 AND version = '%s') AS cand_errors
FROM spans
WHERE %s AND version IN ('%s', '%s')
GROUP BY service, operation
HAVING base_errors = 0 AND cand_errors > 0
ORDER BY cand_errors DESC
LIMIT 20`, base, cand, spanWhere, base, cand)
		newErrors, err = h.ch.Query(r.Context(), newErrSQL)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"service_breakdown": breakdown,
		"top_operations":    topOps,
		"propagation_map":   propagation,
		"new_errors":        newErrors,
	})
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

func buildTraceDrilldown(rows []map[string]any) map[string]any {
	spans := make([]*traceSpan, 0, len(rows))
	byID := map[string]*traceSpan{}
	for _, row := range rows {
		span := &traceSpan{
			TraceID:      toString(row["trace_id"]),
			SpanID:       toString(row["span_id"]),
			ParentSpanID: toString(row["parent_span_id"]),
			Service:      toString(row["service"]),
			Env:          toString(row["env"]),
			Host:         toString(row["host"]),
			Version:      toString(row["version"]),
			Operation:    toString(row["operation"]),
			StartTS:      toString(row["start_ts"]),
			EndTS:        toString(row["end_ts"]),
			DurationMs:   toUint32(row["duration_ms"]),
			SelfTimeMs:   toUint32(row["self_time_ms"]),
			StatusCode:   uint16(toUint32(row["status_code"])),
			IsError:      toFloat(row["is_error"]) > 0,
			Source:       toString(row["source"]),
		}
		if span.SelfTimeMs > span.DurationMs {
			span.SelfTimeMs = span.DurationMs
		}
		if span.DurationMs > span.SelfTimeMs {
			span.WaitMs = span.DurationMs - span.SelfTimeMs
		}
		if span.DurationMs > 0 {
			span.BlockingRatio = float64(span.WaitMs) / float64(span.DurationMs)
		}
		span.StartTime = parseCHTime(span.StartTS)
		span.EndTime = parseCHTime(span.EndTS)
		if span.EndTime.Before(span.StartTime) {
			span.EndTime = span.StartTime
		}
		if span.SpanID != "" {
			byID[span.SpanID] = span
		}
		spans = append(spans, span)
	}

	roots := make([]*traceSpan, 0)
	for _, span := range spans {
		if parent, ok := byID[span.ParentSpanID]; ok && span.ParentSpanID != "" {
			parent.Children = append(parent.Children, span)
		} else {
			roots = append(roots, span)
		}
	}

	var sortTree func(nodes []*traceSpan)
	sortTree = func(nodes []*traceSpan) {
		sort.Slice(nodes, func(i, j int) bool { return nodes[i].StartTime.Before(nodes[j].StartTime) })
		for _, n := range nodes {
			if len(n.Children) > 0 {
				sortTree(n.Children)
			}
		}
	}
	sortTree(roots)

	var setDepth func(nodes []*traceSpan, depth int)
	setDepth = func(nodes []*traceSpan, depth int) {
		for _, n := range nodes {
			n.Depth = depth
			if len(n.Children) > 0 {
				setDepth(n.Children, depth+1)
			}
		}
	}
	setDepth(roots, 0)

	traceStart := time.Now().UTC()
	traceEnd := time.Time{}
	if len(spans) > 0 {
		traceStart = spans[0].StartTime
		traceEnd = spans[0].EndTime
	}
	for _, span := range spans {
		if span.StartTime.Before(traceStart) {
			traceStart = span.StartTime
		}
		if span.EndTime.After(traceEnd) {
			traceEnd = span.EndTime
		}
	}
	totalMs := float64(maxInt64(traceEnd.Sub(traceStart).Milliseconds(), 1))

	criticalIDs := markCriticalPath(roots)
	criticalSet := map[string]struct{}{}
	for _, id := range criticalIDs {
		criticalSet[id] = struct{}{}
	}

	maxWait := uint32(1)
	for _, span := range spans {
		if span.WaitMs > maxWait {
			maxWait = span.WaitMs
		}
	}

	errorChains := make([]map[string]any, 0)
	for _, span := range spans {
		if _, ok := criticalSet[span.SpanID]; ok {
			span.IsCritical = true
		}
		if span.IsError {
			errorChains = append(errorChains, map[string]any{
				"error_span_id": span.SpanID,
				"path":          buildErrorPath(span, byID),
			})
		}
		left := span.StartTime.Sub(traceStart).Milliseconds()
		if left < 0 {
			left = 0
		}
		span.LeftPct = float64(left) / totalMs * 100
		span.WidthPct = math.Max(0.8, float64(span.DurationMs)/totalMs*100)

		waitingOn := ""
		longestChild := uint32(0)
		for _, c := range span.Children {
			if c.DurationMs > longestChild {
				longestChild = c.DurationMs
				waitingOn = c.Service
			}
		}
		if waitingOn != "" {
			span.Explanation = fmt.Sprintf("%s total:%dms self:%dms waiting:%dms on %s(%dms)", span.Service, span.DurationMs, span.SelfTimeMs, span.WaitMs, waitingOn, longestChild)
		} else {
			span.Explanation = fmt.Sprintf("%s total:%dms self:%dms waiting:%dms", span.Service, span.DurationMs, span.SelfTimeMs, span.WaitMs)
		}
	}

	slow := make([]map[string]any, 0, len(spans))
	for _, span := range spans {
		score := 0.6*(float64(span.WaitMs)/float64(maxWait)) + 0.4*span.BlockingRatio
		slow = append(slow, map[string]any{
			"span_id":          span.SpanID,
			"service":          span.Service,
			"operation":        span.Operation,
			"duration_ms":      span.DurationMs,
			"self_time_ms":     span.SelfTimeMs,
			"wait_ms":          span.WaitMs,
			"blocking_ratio":   round(scoreToPct(span.BlockingRatio), 2),
			"score":            round(score, 4),
			"is_critical":      span.IsCritical,
			"is_error":         span.IsError,
			"explanation":      span.Explanation,
			"parent_span_id":   span.ParentSpanID,
			"child_span_count": len(span.Children),
		})
	}
	sort.Slice(slow, func(i, j int) bool {
		return toFloat(slow[i]["score"]) > toFloat(slow[j]["score"])
	})
	if len(slow) > 10 {
		slow = slow[:10]
	}

	waterfall := make([]map[string]any, 0, len(spans))
	sort.Slice(spans, func(i, j int) bool { return spans[i].StartTime.Before(spans[j].StartTime) })
	for _, span := range spans {
		childIDs := make([]string, 0, len(span.Children))
		for _, c := range span.Children {
			childIDs = append(childIDs, c.SpanID)
		}
		waterfall = append(waterfall, map[string]any{
			"trace_id":       span.TraceID,
			"span_id":        span.SpanID,
			"parent_span_id": span.ParentSpanID,
			"service":        span.Service,
			"host":           span.Host,
			"version":        span.Version,
			"operation":      span.Operation,
			"start_ts":       span.StartTS,
			"end_ts":         span.EndTS,
			"duration_ms":    span.DurationMs,
			"self_time_ms":   span.SelfTimeMs,
			"wait_ms":        span.WaitMs,
			"blocking_ratio": round(scoreToPct(span.BlockingRatio), 2),
			"depth":          span.Depth,
			"is_critical":    span.IsCritical,
			"is_error":       span.IsError,
			"left_pct":       round(span.LeftPct, 2),
			"width_pct":      round(span.WidthPct, 2),
			"children":       childIDs,
			"explanation":    span.Explanation,
		})
	}

	return map[string]any{
		"waterfall":     waterfall,
		"critical_path": criticalIDs,
		"error_chains":  errorChains,
		"slow_spots":    slow,
		"trace_window": map[string]any{
			"start_ts": traceStart.UTC().Format("2006-01-02 15:04:05.000"),
			"end_ts":   traceEnd.UTC().Format("2006-01-02 15:04:05.000"),
			"total_ms": uint32(totalMs),
		},
	}
}

func markCriticalPath(roots []*traceSpan) []string {
	if len(roots) == 0 {
		return nil
	}
	root := roots[0]
	for _, r := range roots {
		if r.StartTime.Before(root.StartTime) {
			root = r
		}
	}
	path := []string{}
	curr := root
	for curr != nil {
		path = append(path, curr.SpanID)
		if len(curr.Children) == 0 {
			break
		}
		next := curr.Children[0]
		for _, c := range curr.Children[1:] {
			if c.EndTime.After(next.EndTime) {
				next = c
			}
		}
		curr = next
	}
	return path
}

func buildErrorPath(errSpan *traceSpan, byID map[string]*traceSpan) []string {
	path := []string{}
	cur := errSpan
	for cur != nil {
		path = append([]string{fmt.Sprintf("%s(%s)", cur.Service, cur.SpanID)}, path...)
		if cur.ParentSpanID == "" {
			break
		}
		next := byID[cur.ParentSpanID]
		if next == nil {
			break
		}
		cur = next
	}
	return path
}

func buildRootCauseRanking(rows []map[string]any, base, cand string) []rootCauseRank {
	type stats struct {
		Calls         float64
		P95           float64
		ErrorRate     float64
		BlockingRatio float64
	}
	baseStats := map[string]stats{}
	candStats := map[string]stats{}

	for _, row := range rows {
		s := stats{
			Calls:         toFloat(row["calls"]),
			P95:           toFloat(row["p95_ms"]),
			ErrorRate:     toFloat(row["error_rate"]),
			BlockingRatio: toFloat(row["blocking_ratio"]),
		}
		svc := toString(row["service"])
		version := toString(row["version"])
		if version == base {
			baseStats[svc] = s
		}
		if version == cand {
			candStats[svc] = s
		}
	}

	services := map[string]struct{}{}
	for svc := range baseStats {
		services[svc] = struct{}{}
	}
	for svc := range candStats {
		services[svc] = struct{}{}
	}

	out := make([]rootCauseRank, 0, len(services))
	for svc := range services {
		b := baseStats[svc]
		c := candStats[svc]
		latPct := pctDelta(b.P95, c.P95)
		errPct := pctDelta(b.ErrorRate, c.ErrorRate)
		callPct := pctDelta(b.Calls, c.Calls)
		score := 0.5*clamp(latPct/300, 0, 1) + 0.25*clamp(errPct/300, 0, 1) + 0.15*clamp(callPct/300, 0, 1) + 0.10*clamp(c.BlockingRatio, 0, 1)
		reason := fmt.Sprintf("latency %+0.1f%%, error %+0.1f%%, calls %+0.1f%%", latPct, errPct, callPct)
		out = append(out, rootCauseRank{
			Service:         svc,
			Score:           round(score, 4),
			LatencyDeltaPct: round(latPct, 2),
			ErrorDeltaPct:   round(errPct, 2),
			CallDeltaPct:    round(callPct, 2),
			BlockingRatio:   round(c.BlockingRatio, 4),
			Reason:          reason,
		})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Score > out[j].Score })
	if len(out) > 10 {
		out = out[:10]
	}
	return out
}

func buildAnomalyBadges(rows []map[string]any) []map[string]any {
	if len(rows) == 0 {
		return nil
	}
	r := rows[0]
	baseP95 := toFloat(r["base_p95"])
	candP95 := toFloat(r["cand_p95"])
	baseErr := toFloat(r["base_error_rate"])
	candErr := toFloat(r["cand_error_rate"])
	baseCalls := toFloat(r["base_calls"])
	candCalls := toFloat(r["cand_calls"])

	latPct := pctDelta(baseP95, candP95)
	errPct := pctDelta(baseErr, candErr)
	callPct := pctDelta(baseCalls, candCalls)

	deviation := clamp(math.Max(math.Abs(latPct)/300, math.Max(math.Abs(errPct)/300, math.Abs(callPct)/300)), 0, 1)
	badges := make([]map[string]any, 0)
	if latPct >= 100 {
		badges = append(badges, map[string]any{
			"level":           "orange",
			"title":           "Latency spike detected",
			"message":         fmt.Sprintf("p95 +%.1f%%", latPct),
			"deviation_score": round(deviation, 3),
		})
	}
	if errPct >= 50 {
		badges = append(badges, map[string]any{
			"level":           "red",
			"title":           "Error anomaly detected",
			"message":         fmt.Sprintf("error rate +%.1f%%", errPct),
			"deviation_score": round(deviation, 3),
		})
	}
	if callPct >= 100 {
		badges = append(badges, map[string]any{
			"level":           "yellow",
			"title":           "Traffic spike detected",
			"message":         fmt.Sprintf("calls +%.1f%%", callPct),
			"deviation_score": round(deviation, 3),
		})
	}
	return badges
}

func toString(v any) string {
	switch t := v.(type) {
	case nil:
		return ""
	case string:
		return t
	case fmt.Stringer:
		return t.String()
	default:
		return fmt.Sprintf("%v", t)
	}
}

func toFloat(v any) float64 {
	switch t := v.(type) {
	case nil:
		return 0
	case float64:
		return t
	case float32:
		return float64(t)
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case uint64:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(t), 64)
		if err != nil {
			return 0
		}
		return f
	default:
		f, _ := strconv.ParseFloat(fmt.Sprintf("%v", t), 64)
		return f
	}
}

func toUint32(v any) uint32 {
	f := toFloat(v)
	if f <= 0 {
		return 0
	}
	if f > float64(math.MaxUint32) {
		return math.MaxUint32
	}
	return uint32(math.Round(f))
}

func parseCHTime(v string) time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Now().UTC()
	}
	formats := []string{"2006-01-02 15:04:05.000", "2006-01-02 15:04:05", time.RFC3339Nano}
	for _, f := range formats {
		if parsed, err := time.Parse(f, v); err == nil {
			return parsed.UTC()
		}
	}
	return time.Now().UTC()
}

func pctDelta(base, cand float64) float64 {
	if base == 0 {
		if cand == 0 {
			return 0
		}
		return 100
	}
	return ((cand - base) / math.Abs(base)) * 100
}

func clamp(v, min, max float64) float64 {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func round(v float64, digits int) float64 {
	pow := math.Pow(10, float64(digits))
	return math.Round(v*pow) / pow
}

func scoreToPct(v float64) float64 {
	return v * 100
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}
