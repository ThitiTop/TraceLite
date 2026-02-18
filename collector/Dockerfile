FROM golang:1.26-alpine AS build
WORKDIR /app
COPY go.mod ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/collector ./cmd/collector

FROM gcr.io/distroless/static-debian12
COPY --from=build /bin/collector /collector
EXPOSE 8443
ENTRYPOINT ["/collector"]

