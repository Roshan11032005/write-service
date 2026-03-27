FROM golang:1.22-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build both binaries
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/write-service   ./cmd/service
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/index-builder   ./cmd/indexer

# ── Write Service image ──────────────────────────────────────────────────────
FROM alpine:3.20 AS write-service

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/write-service /usr/local/bin/write-service

EXPOSE 8085
ENTRYPOINT ["write-service"]

# ── Index Builder image ──────────────────────────────────────────────────────
FROM alpine:3.20 AS index-builder

RUN apk add --no-cache ca-certificates tzdata
COPY --from=builder /out/index-builder /usr/local/bin/index-builder

ENTRYPOINT ["index-builder"]
