# Build from repository root (see deployments/docker-compose.yml).
FROM golang:1.23-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/helios ./cmd/helios

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates wget \
	&& rm -rf /var/lib/apt/lists/*
COPY --from=build /out/helios /usr/local/bin/helios
EXPOSE 8080 7000 9090
ENTRYPOINT ["/usr/local/bin/helios"]
