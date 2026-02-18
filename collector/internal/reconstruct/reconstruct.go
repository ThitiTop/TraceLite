package reconstruct

import (
	"context"
	"sort"
	"sync"
	"time"

	"trace-lite/collector/internal/clickhouse"
	"trace-lite/collector/internal/model"
)

type Reconstructor struct {
	mu            sync.Mutex
	traces        map[string]*traceState
	window        time.Duration
	flushInterval time.Duration
	ch            *clickhouse.Client
}

type traceState struct {
	id        string
	env       string
	updatedAt time.Time
	spans     map[string]*spanState
}

type spanState struct {
	traceID      string
	spanID       string
	parentSpanID string
	service      string
	env          string
	host         string
	version      string
	operation    string
	startTs      time.Time
	endTs        time.Time
	durationMs   uint32
	statusCode   uint16
	isError      bool
	source       string
}

func New(ch *clickhouse.Client, window, flushInterval time.Duration) *Reconstructor {
	return &Reconstructor{
		traces:        map[string]*traceState{},
		window:        window,
		flushInterval: flushInterval,
		ch:            ch,
	}
}

func (r *Reconstructor) Add(rows []model.RawLogRow, eventTimes []time.Time) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for i, row := range rows {
		ts := eventTimes[i]
		t := r.traces[row.TraceID]
		if t == nil {
			t = &traceState{
				id:    row.TraceID,
				env:   row.Env,
				spans: map[string]*spanState{},
			}
			r.traces[row.TraceID] = t
		}
		if ts.After(t.updatedAt) {
			t.updatedAt = ts
		}

		spanID := row.SpanID
		if spanID == "" {
			spanID = "implicit-" + model.FormatCHTime(ts)
		}
		s := t.spans[spanID]
		if s == nil {
			s = &spanState{
				traceID:      row.TraceID,
				spanID:       spanID,
				parentSpanID: row.ParentSpanID,
				service:      row.Service,
				env:          row.Env,
				host:         row.Host,
				version:      row.Version,
				operation:    chooseOperation(row.Route, row.Message),
				source:       "explicit",
			}
			t.spans[spanID] = s
		}

		if row.ParentSpanID != "" {
			s.parentSpanID = row.ParentSpanID
		}
		if s.service == "" {
			s.service = row.Service
		}
		if s.version == "" {
			s.version = row.Version
		}
		if s.host == "" {
			s.host = row.Host
		}
		if s.operation == "" {
			s.operation = chooseOperation(row.Route, row.Message)
		}
		if row.StatusCode >= 400 {
			s.isError = true
			s.statusCode = row.StatusCode
		}
		if row.StatusCode > 0 {
			s.statusCode = row.StatusCode
		}

		switch row.Event {
		case "start":
			if s.startTs.IsZero() || ts.Before(s.startTs) {
				s.startTs = ts
			}
		case "end":
			if s.endTs.IsZero() || ts.After(s.endTs) {
				s.endTs = ts
			}
			if row.DurationMs > 0 {
				s.durationMs = row.DurationMs
			}
		default:
			if row.DurationMs > 0 {
				if s.endTs.IsZero() || ts.After(s.endTs) {
					s.endTs = ts
				}
				candidateStart := ts.Add(-time.Duration(row.DurationMs) * time.Millisecond)
				if s.startTs.IsZero() || candidateStart.Before(s.startTs) {
					s.startTs = candidateStart
				}
				s.durationMs = row.DurationMs
			}
		}
	}
}

func (r *Reconstructor) Run(ctx context.Context) {
	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.FlushNow(ctx)
		}
	}
}

func (r *Reconstructor) FlushNow(ctx context.Context) {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := time.Now().UTC()
	var spanRows []model.SpanRow
	var traceRows []model.TraceRow
	edgeAgg := map[edgeKey]*edgeState{}

	for traceID, t := range r.traces {
		if now.Sub(t.updatedAt) < r.window {
			continue
		}

		spans := finalizeSpans(t)
		if len(spans) == 0 {
			delete(r.traces, traceID)
			continue
		}
		spanRows = append(spanRows, spans...)
		traceRows = append(traceRows, buildTraceRow(t.env, traceID, spans))
		accumulateEdges(spans, edgeAgg)
		delete(r.traces, traceID)
	}

	if len(spanRows) > 0 {
		_ = r.ch.InsertJSONEachRow(ctx, "spans", spanRows)
	}
	if len(traceRows) > 0 {
		_ = r.ch.InsertJSONEachRow(ctx, "traces", traceRows)
	}
	if len(edgeAgg) > 0 {
		edges := collapseEdgeAgg(edgeAgg)
		_ = r.ch.InsertJSONEachRow(ctx, "dependency_edges_minute", edges)
	}
}

func chooseOperation(route, fallback string) string {
	if route != "" {
		return route
	}
	if fallback != "" {
		return fallback
	}
	return "unknown-op"
}

func finalizeSpans(t *traceState) []model.SpanRow {
	children := map[string][]*spanState{}
	for _, s := range t.spans {
		if s.parentSpanID != "" {
			children[s.parentSpanID] = append(children[s.parentSpanID], s)
		}
	}

	out := make([]model.SpanRow, 0, len(t.spans))
	for _, s := range t.spans {
		source := s.source
		if s.startTs.IsZero() && !s.endTs.IsZero() && s.durationMs > 0 {
			s.startTs = s.endTs.Add(-time.Duration(s.durationMs) * time.Millisecond)
			source = "inferred"
		}
		if s.endTs.IsZero() && !s.startTs.IsZero() {
			if s.durationMs > 0 {
				s.endTs = s.startTs.Add(time.Duration(s.durationMs) * time.Millisecond)
			} else {
				s.endTs = s.startTs
			}
			source = "inferred"
		}
		if s.startTs.IsZero() {
			s.startTs = time.Now().UTC()
			s.endTs = s.startTs
			source = "inferred"
		}

		duration := s.durationMs
		if duration == 0 {
			if s.endTs.Before(s.startTs) {
				s.endTs = s.startTs
			}
			duration = uint32(s.endTs.Sub(s.startTs).Milliseconds())
		}

		childTotal := uint32(0)
		for _, child := range children[s.spanID] {
			childDur := child.durationMs
			if childDur == 0 && !child.startTs.IsZero() && !child.endTs.IsZero() {
				childDur = uint32(child.endTs.Sub(child.startTs).Milliseconds())
			}
			childTotal += childDur
		}
		selfTime := duration
		if childTotal < duration {
			selfTime = duration - childTotal
		}

		out = append(out, model.SpanRow{
			TraceID:      s.traceID,
			SpanID:       s.spanID,
			ParentSpanID: s.parentSpanID,
			Service:      s.service,
			Env:          s.env,
			Host:         s.host,
			Version:      s.version,
			Operation:    s.operation,
			StartTS:      model.FormatCHTime(s.startTs),
			EndTS:        model.FormatCHTime(s.endTs),
			DurationMs:   duration,
			SelfTimeMs:   selfTime,
			StatusCode:   s.statusCode,
			IsError:      boolToUint8(s.isError),
			Source:       source,
		})
	}
	return out
}

func buildTraceRow(env, traceID string, spans []model.SpanRow) model.TraceRow {
	if len(spans) == 0 {
		return model.TraceRow{TraceID: traceID, Env: env}
	}

	start := parseCHTime(spans[0].StartTS)
	end := parseCHTime(spans[0].EndTS)
	services := map[string]struct{}{}
	versions := map[string]struct{}{}
	errorCount := 0
	rootService := spans[0].Service
	byID := map[string]model.SpanRow{}
	children := map[string][]string{}
	for _, s := range spans {
		byID[s.SpanID] = s
		if s.ParentSpanID != "" {
			children[s.ParentSpanID] = append(children[s.ParentSpanID], s.SpanID)
		}
		st := parseCHTime(s.StartTS)
		en := parseCHTime(s.EndTS)
		if st.Before(start) {
			start = st
			rootService = s.Service
		}
		if en.After(end) {
			end = en
		}
		services[s.Service] = struct{}{}
		versions[s.Version] = struct{}{}
		if s.IsError == 1 {
			errorCount++
		}
	}

	critical := criticalPath(byID, children)
	versionsOut := make([]string, 0, len(versions))
	for v := range versions {
		versionsOut = append(versionsOut, v)
	}
	sort.Strings(versionsOut)

	return model.TraceRow{
		TraceID:        traceID,
		Env:            env,
		RootService:    rootService,
		StartTS:        model.FormatCHTime(start),
		EndTS:          model.FormatCHTime(end),
		DurationMs:     uint32(end.Sub(start).Milliseconds()),
		SpanCount:      uint16(len(spans)),
		ServiceCount:   uint16(len(services)),
		ErrorCount:     uint16(errorCount),
		CriticalPathMs: critical,
		Versions:       versionsOut,
	}
}

func criticalPath(spans map[string]model.SpanRow, children map[string][]string) uint32 {
	memo := map[string]uint32{}
	visiting := map[string]bool{}

	var dfs func(string) uint32
	dfs = func(id string) uint32 {
		if v, ok := memo[id]; ok {
			return v
		}
		if visiting[id] {
			return 0
		}
		visiting[id] = true
		s := spans[id]
		bestChild := uint32(0)
		for _, c := range children[id] {
			if childScore := dfs(c); childScore > bestChild {
				bestChild = childScore
			}
		}
		visiting[id] = false
		total := s.DurationMs + bestChild
		memo[id] = total
		return total
	}

	best := uint32(0)
	for id, s := range spans {
		if s.ParentSpanID != "" {
			if _, ok := spans[s.ParentSpanID]; ok {
				continue
			}
		}
		if score := dfs(id); score > best {
			best = score
		}
	}
	if best == 0 {
		for id := range spans {
			if score := dfs(id); score > best {
				best = score
			}
		}
	}
	return best
}

type edgeKey struct {
	bucket        string
	env           string
	callerService string
	calleeService string
	callerVersion string
	calleeVersion string
}

type edgeState struct {
	durations  []uint32
	errorCalls uint64
}

func accumulateEdges(spans []model.SpanRow, agg map[edgeKey]*edgeState) {
	byID := map[string]model.SpanRow{}
	for _, s := range spans {
		byID[s.SpanID] = s
	}
	for _, s := range spans {
		if s.ParentSpanID == "" {
			continue
		}
		p, ok := byID[s.ParentSpanID]
		if !ok || p.Service == s.Service {
			continue
		}
		bucket := toMinute(s.StartTS)
		k := edgeKey{
			bucket:        bucket,
			env:           s.Env,
			callerService: p.Service,
			calleeService: s.Service,
			callerVersion: p.Version,
			calleeVersion: s.Version,
		}
		e := agg[k]
		if e == nil {
			e = &edgeState{}
			agg[k] = e
		}
		e.durations = append(e.durations, s.DurationMs)
		if s.IsError == 1 {
			e.errorCalls++
		}
	}
}

func collapseEdgeAgg(agg map[edgeKey]*edgeState) []model.DependencyEdgeRow {
	out := make([]model.DependencyEdgeRow, 0, len(agg))
	for k, v := range agg {
		sort.Slice(v.durations, func(i, j int) bool { return v.durations[i] < v.durations[j] })
		calls := len(v.durations)
		if calls == 0 {
			continue
		}
		p50 := percentile(v.durations, 0.50)
		p95 := percentile(v.durations, 0.95)
		maxV := v.durations[calls-1]
		out = append(out, model.DependencyEdgeRow{
			BucketTS:      k.bucket,
			Env:           k.env,
			CallerService: k.callerService,
			CalleeService: k.calleeService,
			CallerVersion: k.callerVersion,
			CalleeVersion: k.calleeVersion,
			Calls:         uint64(calls),
			ErrorCalls:    v.errorCalls,
			P50Ms:         float32(p50),
			P95Ms:         float32(p95),
			MaxMs:         maxV,
		})
	}
	return out
}

func percentile(arr []uint32, p float64) float64 {
	if len(arr) == 0 {
		return 0
	}
	idx := int(float64(len(arr)-1) * p)
	if idx < 0 {
		idx = 0
	}
	if idx >= len(arr) {
		idx = len(arr) - 1
	}
	return float64(arr[idx])
}

func toMinute(chTS string) string {
	t := parseCHTime(chTS)
	return t.UTC().Format("2006-01-02 15:04:00")
}

func parseCHTime(v string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05.000", v)
	if err != nil {
		return time.Now().UTC()
	}
	return t.UTC()
}

func boolToUint8(v bool) uint8 {
	if v {
		return 1
	}
	return 0
}
