# syntax=docker/dockerfile:1

# Многостадийная сборка демо-сервиса LSI (Go 1.23, статический бинарник).
FROM golang:1.23-alpine AS build
WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/liquidity-demo ./cmd/app

FROM alpine:3.19
WORKDIR /app

RUN apk add --no-cache ca-certificates

COPY --from=build /out/liquidity-demo /usr/local/bin/liquidity-demo
COPY migrations ./migrations
COPY data ./data

ENV APP_ROOT=/app
ENV LISTEN_ADDR=:8080
ENV DATABASE_URL=postgres://lsi:lsi@postgres:5432/lsi?sslmode=disable

ENTRYPOINT ["/usr/local/bin/liquidity-demo"]
