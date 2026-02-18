.PHONY: up down logs init-schema test build

up:
	docker compose -f deploy/docker-compose.yml up --build -d

down:
	docker compose -f deploy/docker-compose.yml down

logs:
	docker compose -f deploy/docker-compose.yml logs -f --tail=200

init-schema:
	docker compose -f deploy/docker-compose.yml exec clickhouse clickhouse-client --queries-file /docker-entrypoint-initdb.d/001_schema.sql

test:
	cd collector && go test ./...
	cd api && go test ./...
	cd ui && npm.cmd run build

build:
	docker compose -f deploy/docker-compose.yml build
