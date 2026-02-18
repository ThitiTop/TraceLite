package model

import (
	"fmt"
	"strings"
	"time"
)

type IngestEvent struct {
	Timestamp     string            `json:"timestamp"`
	Service       string            `json:"service"`
	Env           string            `json:"env"`
	Host          string            `json:"host"`
	Level         string            `json:"level"`
	Message       string            `json:"message"`
	Status        string            `json:"status"`
	CorrelationID string            `json:"correlationId"`
	SpanID        string            `json:"spanId"`
	ParentSpanID  string            `json:"parentSpanId"`
	Event         string            `json:"event"`
	Route         string            `json:"route"`
	Method        string            `json:"method"`
	StatusCode    uint16            `json:"statusCode"`
	DurationMs    uint32            `json:"durationMs"`
	Version       string            `json:"version"`
	Attrs         map[string]string `json:"attrs"`
}

type RawLogRow struct {
	TS           string            `json:"ts"`
	Service      string            `json:"service"`
	Env          string            `json:"env"`
	Host         string            `json:"host"`
	Version      string            `json:"version"`
	Level        string            `json:"level"`
	Message      string            `json:"message"`
	TraceID      string            `json:"trace_id"`
	SpanID       string            `json:"span_id"`
	ParentSpanID string            `json:"parent_span_id"`
	Event        string            `json:"event"`
	Route        string            `json:"route"`
	Method       string            `json:"method"`
	StatusCode   uint16            `json:"status_code"`
	DurationMs   uint32            `json:"duration_ms"`
	Attrs        map[string]string `json:"attrs"`
	RawJSON      string            `json:"raw_json"`
}

type SpanRow struct {
	TraceID      string `json:"trace_id"`
	SpanID       string `json:"span_id"`
	ParentSpanID string `json:"parent_span_id"`
	Service      string `json:"service"`
	Env          string `json:"env"`
	Host         string `json:"host"`
	Version      string `json:"version"`
	Operation    string `json:"operation"`
	StartTS      string `json:"start_ts"`
	EndTS        string `json:"end_ts"`
	DurationMs   uint32 `json:"duration_ms"`
	SelfTimeMs   uint32 `json:"self_time_ms"`
	StatusCode   uint16 `json:"status_code"`
	IsError      uint8  `json:"is_error"`
	Source       string `json:"source"`
}

type TraceRow struct {
	TraceID        string   `json:"trace_id"`
	Env            string   `json:"env"`
	RootService    string   `json:"root_service"`
	StartTS        string   `json:"start_ts"`
	EndTS          string   `json:"end_ts"`
	DurationMs     uint32   `json:"duration_ms"`
	SpanCount      uint16   `json:"span_count"`
	ServiceCount   uint16   `json:"service_count"`
	ErrorCount     uint16   `json:"error_count"`
	CriticalPathMs uint32   `json:"critical_path_ms"`
	Versions       []string `json:"versions"`
}

type DependencyEdgeRow struct {
	BucketTS      string  `json:"bucket_ts"`
	Env           string  `json:"env"`
	CallerService string  `json:"caller_service"`
	CalleeService string  `json:"callee_service"`
	CallerVersion string  `json:"caller_version"`
	CalleeVersion string  `json:"callee_version"`
	Calls         uint64  `json:"calls"`
	ErrorCalls    uint64  `json:"error_calls"`
	P50Ms         float32 `json:"p50_ms"`
	P95Ms         float32 `json:"p95_ms"`
	MaxMs         uint32  `json:"max_ms"`
}

func (e IngestEvent) ToRaw(raw string) (RawLogRow, time.Time, error) {
	traceID := strings.TrimSpace(e.CorrelationID)
	if traceID == "" {
		return RawLogRow{}, time.Time{}, fmt.Errorf("missing correlationId")
	}

	ts := time.Now().UTC()
	if strings.TrimSpace(e.Timestamp) != "" {
		parsed, err := time.Parse(time.RFC3339Nano, e.Timestamp)
		if err != nil {
			return RawLogRow{}, time.Time{}, fmt.Errorf("invalid timestamp: %w", err)
		}
		ts = parsed.UTC()
	}

	eventType := strings.ToLower(strings.TrimSpace(e.Event))
	if eventType == "" {
		eventType = "log"
	}

	attrs := e.Attrs
	if attrs == nil {
		attrs = map[string]string{}
	}
	if s := strings.TrimSpace(e.Status); s != "" {
		attrs["status"] = strings.ToUpper(s)
	}

	row := RawLogRow{
		TS:           FormatCHTime(ts),
		Service:      withDefault(e.Service, "unknown-service"),
		Env:          withDefault(e.Env, "unknown"),
		Host:         withDefault(e.Host, "unknown-host"),
		Version:      withDefault(e.Version, "unknown"),
		Level:        strings.ToUpper(withDefault(e.Level, "INFO")),
		Message:      e.Message,
		TraceID:      traceID,
		SpanID:       strings.TrimSpace(e.SpanID),
		ParentSpanID: strings.TrimSpace(e.ParentSpanID),
		Event:        eventType,
		Route:        e.Route,
		Method:       strings.ToUpper(e.Method),
		StatusCode:   e.StatusCode,
		DurationMs:   e.DurationMs,
		Attrs:        attrs,
		RawJSON:      raw,
	}
	return row, ts, nil
}

func withDefault(v, fallback string) string {
	if strings.TrimSpace(v) == "" {
		return fallback
	}
	return strings.TrimSpace(v)
}

func FormatCHTime(t time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000")
}
