package clickhouse

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Client struct {
	baseURL    string
	database   string
	httpClient *http.Client
}

type queryResponse struct {
	Data []map[string]any `json:"data"`
}

func NewClient(baseURL, database string) *Client {
	return &Client{
		baseURL:  strings.TrimRight(baseURL, "/"),
		database: database,
		httpClient: &http.Client{
			Timeout: 20 * time.Second,
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
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("clickhouse ping failed: %s (%s)", resp.Status, string(body))
	}
	return nil
}

func (c *Client) Query(ctx context.Context, sql string) ([]map[string]any, error) {
	statement := fmt.Sprintf("%s FORMAT JSON", strings.TrimSuffix(strings.TrimSpace(sql), ";"))
	queryURL := fmt.Sprintf("%s/?database=%s", c.baseURL, url.QueryEscape(c.database))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, queryURL, bytes.NewBufferString(statement))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "text/plain")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 8192))
		return nil, fmt.Errorf("query failed: %s (%s)", resp.Status, string(body))
	}
	var out queryResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return out.Data, nil
}
