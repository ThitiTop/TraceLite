package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	database   string
	httpClient *http.Client
}

func NewClient(baseURL, database string) *Client {
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		database: database,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (c *Client) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/ping", nil)
	if err != nil {
		return err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("clickhouse ping failed: %s (%s)", resp.Status, string(b))
	}
	return nil
}

func (c *Client) InsertJSONEachRow(ctx context.Context, table string, rows any) error {
	payload, err := toNDJSON(rows)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		return nil
	}

	query := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", c.database, table)
	insertURL := fmt.Sprintf("%s/?query=%s", c.baseURL, url.QueryEscape(query))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, insertURL, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return fmt.Errorf("clickhouse insert failed: %s (%s)", resp.Status, string(b))
	}
	return nil
}

func toNDJSON(rows any) ([]byte, error) {
	v := reflectRows(rows)
	if len(v) == 0 {
		return nil, nil
	}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	for _, item := range v {
		if err := enc.Encode(item); err != nil {
			return nil, err
		}
	}
	return buf.Bytes(), nil
}

func reflectRows(rows any) []any {
	switch r := rows.(type) {
	case []map[string]any:
		out := make([]any, 0, len(r))
		for i := range r {
			out = append(out, r[i])
		}
		return out
	}

	val := reflect.ValueOf(rows)
	if val.Kind() != reflect.Slice {
		return nil
	}
	out := make([]any, 0, val.Len())
	for i := 0; i < val.Len(); i++ {
		out = append(out, val.Index(i).Interface())
	}
	return out
}
