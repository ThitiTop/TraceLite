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
	if err := http.ListenAndServe(cfg.Addr, withCORS(mux)); err != nil {
		log.Fatalf("listen failed: %v", err)
	}
}

func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type,Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}
