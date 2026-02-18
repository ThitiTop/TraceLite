package config

import "os"

type Config struct {
	Addr          string
	ClickHouseDSN string
	ClickHouseDB  string
}

func Load() Config {
	return Config{
		Addr:          getEnv("API_ADDR", ":8080"),
		ClickHouseDSN: getEnv("CLICKHOUSE_DSN", "http://localhost:8123"),
		ClickHouseDB:  getEnv("CLICKHOUSE_DB", "trace_lite"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
