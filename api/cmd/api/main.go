package main

import (
	"log"
	"net/http"

	"trace-lite/api/internal/clickhouse"
	"trace-lite/api/internal/config"
	"trace-lite/api/internal/handlers"
)

func main() {
	cfg := config.Load()
	ch := clickhouse.NewClient(cfg.ClickHouseDSN, cfg.ClickHouseDB)
	h := handlers.New(ch)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/healthz", h.Healthz)
	mux.HandleFunc("/v1/traces", h.Traces)
	mux.HandleFunc("/v1/traces/", h.TraceByID)
	mux.HandleFunc("/v1/dependency", h.Dependency)
	mux.HandleFunc("/v1/hosts", h.Hosts)
	mux.HandleFunc("/v1/compare", h.Compare)

	log.Printf("api listening on %s", cfg.Addr)
	if err := http.ListenAndServe(cfg.Addr, mux); err != nil {
		log.Fatalf("listen failed: %v", err)
	}
}
