package server

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"time"

	"trace-lite/collector/internal/clickhouse"
	"trace-lite/collector/internal/model"
	"trace-lite/collector/internal/reconstruct"
)

type Handler struct {
	token string
	ch    *clickhouse.Client
	recon *reconstruct.Reconstructor
}

type ingestError struct {
	Line   int    `json:"line"`
	Reason string `json:"reason"`
}

type ingestResponse struct {
	Accepted int           `json:"accepted"`
	Rejected int           `json:"rejected"`
	Errors   []ingestError `json:"errors,omitempty"`
}

func NewHandler(token string, ch *clickhouse.Client, recon *reconstruct.Reconstructor) *Handler {
	return &Handler{token: token, ch: ch, recon: recon}
}

func (h *Handler) Healthz(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := h.ch.Ping(ctx); err != nil {
		http.Error(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}

func (h *Handler) IngestLogs(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if h.token != "" && !validBearer(r.Header.Get("Authorization"), h.token) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}

	reader, err := maybeGzipReader(r)
	if err != nil {
		http.Error(w, "invalid gzip", http.StatusBadRequest)
		return
	}
	defer reader.Close()

	events, raws, parseErrs := parseEvents(reader)
	resp := ingestResponse{Errors: parseErrs}
	if len(events) == 0 {
		resp.Rejected = len(parseErrs)
		writeJSON(w, http.StatusBadRequest, resp)
		return
	}

	rawRows := make([]model.RawLogRow, 0, len(events))
	times := make([]time.Time, 0, len(events))
	for i := range events {
		row, ts, err := events[i].ToRaw(raws[i])
		if err != nil {
			resp.Rejected++
			if len(resp.Errors) < 100 {
				resp.Errors = append(resp.Errors, ingestError{Line: i + 1, Reason: err.Error()})
			}
			continue
		}
		rawRows = append(rawRows, row)
		times = append(times, ts)
	}

	if len(rawRows) > 0 {
		if err := h.ch.InsertJSONEachRow(r.Context(), "raw_logs", rawRows); err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		h.recon.Add(rawRows, times)
		resp.Accepted = len(rawRows)
	}
	resp.Rejected += len(events) - len(rawRows)
	writeJSON(w, http.StatusOK, resp)
}

func maybeGzipReader(r *http.Request) (io.ReadCloser, error) {
	if strings.EqualFold(r.Header.Get("Content-Encoding"), "gzip") {
		gz, err := gzip.NewReader(r.Body)
		if err != nil {
			return nil, err
		}
		return &compositeReadCloser{Reader: gz, closers: []io.Closer{gz, r.Body}}, nil
	}
	return r.Body, nil
}

type compositeReadCloser struct {
	io.Reader
	closers []io.Closer
}

func (c *compositeReadCloser) Close() error {
	for _, cl := range c.closers {
		_ = cl.Close()
	}
	return nil
}

func parseEvents(r io.Reader) ([]model.IngestEvent, []string, []ingestError) {
	body, err := io.ReadAll(io.LimitReader(r, 20*1024*1024))
	if err != nil {
		return nil, nil, []ingestError{{Line: 0, Reason: err.Error()}}
	}

	trimmed := strings.TrimSpace(string(body))
	if trimmed == "" {
		return nil, nil, []ingestError{{Line: 0, Reason: "empty body"}}
	}

	if strings.HasPrefix(trimmed, "[") {
		var rawMsgs []json.RawMessage
		if err := json.Unmarshal([]byte(trimmed), &rawMsgs); err != nil {
			return nil, nil, []ingestError{{Line: 0, Reason: err.Error()}}
		}
		events := make([]model.IngestEvent, 0, len(rawMsgs))
		raws := make([]string, 0, len(rawMsgs))
		errs := make([]ingestError, 0)
		for i, m := range rawMsgs {
			var e model.IngestEvent
			if err := json.Unmarshal(m, &e); err != nil {
				errs = append(errs, ingestError{Line: i + 1, Reason: err.Error()})
				continue
			}
			events = append(events, e)
			raws = append(raws, string(m))
		}
		return events, raws, errs
	}

	if strings.Contains(trimmed, "\n") {
		scanner := bufio.NewScanner(strings.NewReader(trimmed))
		scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
		events := make([]model.IngestEvent, 0)
		raws := make([]string, 0)
		errs := make([]ingestError, 0)
		line := 0
		for scanner.Scan() {
			line++
			entry := strings.TrimSpace(scanner.Text())
			if entry == "" {
				continue
			}
			var e model.IngestEvent
			if err := json.Unmarshal([]byte(entry), &e); err != nil {
				errs = append(errs, ingestError{Line: line, Reason: err.Error()})
				continue
			}
			events = append(events, e)
			raws = append(raws, entry)
		}
		if err := scanner.Err(); err != nil {
			errs = append(errs, ingestError{Line: line, Reason: err.Error()})
		}
		return events, raws, errs
	}

	var single model.IngestEvent
	if err := json.Unmarshal([]byte(trimmed), &single); err != nil {
		return nil, nil, []ingestError{{Line: 1, Reason: err.Error()}}
	}
	return []model.IngestEvent{single}, []string{trimmed}, nil
}

func validBearer(header, token string) bool {
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 {
		return false
	}
	if !strings.EqualFold(parts[0], "Bearer") {
		return false
	}
	return strings.TrimSpace(parts[1]) == token
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
