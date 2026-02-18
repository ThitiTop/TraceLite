package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	Addr              string
	ClickHouseDSN     string
	ClickHouseDB      string
	IngestToken       string
	TLSAutoSelfSigned bool
	TLSCertFile       string
	TLSKeyFile        string
	TraceWindow       time.Duration
	FlushInterval     time.Duration
}

func Load() Config {
	return Config{
		Addr:              getEnv("COLLECTOR_ADDR", ":8443"),
		ClickHouseDSN:     getEnv("CLICKHOUSE_DSN", "http://localhost:8123"),
		ClickHouseDB:      getEnv("CLICKHOUSE_DB", "trace_lite"),
		IngestToken:       getEnv("INGEST_TOKEN", ""),
		TLSAutoSelfSigned: getEnvBool("TLS_AUTO_SELF_SIGNED", true),
		TLSCertFile:       os.Getenv("TLS_CERT_FILE"),
		TLSKeyFile:        os.Getenv("TLS_KEY_FILE"),
		TraceWindow:       getEnvDuration("TRACE_WINDOW", 2*time.Minute),
		FlushInterval:     getEnvDuration("FLUSH_INTERVAL", 10*time.Second),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvBool(key string, fallback bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return b
}

func getEnvDuration(key string, fallback time.Duration) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return fallback
	}
	return d
}
