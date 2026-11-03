# Build layer
FROM golang:1.27-alpine AS builder

ARG RESEC_VERSION=local-dev

RUN apk --no-cache add ca-certificates && \
    addgroup -g 1000 -S app && \
    adduser -S -G app -u 1000 -H app && \
    mkdir -p /out/app && chown 1000:1000 /out/app

WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    CGO_ENABLED=0 GOOS=linux \
    go build -trimpath -ldflags="-s -w -X 'main.Version=${RESEC_VERSION}'" -o /out/app/resec .

# Run layer
FROM scratch
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /etc/passwd /etc/group /etc/
COPY --from=builder --chown=1000:1000 /out/app /app
USER 1000:1000
WORKDIR /app
ENTRYPOINT ["/app/resec"]
