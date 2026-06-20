# syntax=docker/dockerfile:1

# ---- build stage ---------------------------------------------------------
FROM golang:1.26-alpine AS build

WORKDIR /src

# Download dependencies first so they are cached independently of source changes.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

# Build a fully static, stripped binary for the API gateway.
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/api ./cmd/api

# ---- runtime stage -------------------------------------------------------
# distroless static + nonroot: no shell, no package manager, runs as uid 65532.
FROM gcr.io/distroless/static-debian12:nonroot

WORKDIR /
COPY --from=build /out/api /api

EXPOSE 8080
USER 65532:65532
ENTRYPOINT ["/api"]
