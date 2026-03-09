# Build stage
FROM golang:1.24.4-alpine AS builder

# Set the working directory
WORKDIR /app

# Install git and other build dependencies
RUN apk add --no-cache git tzdata gcc musl-dev

# Set Go env
ENV GO111MODULE=on \
    GOPROXY=https://goproxy.io,direct

# Copy go.mod and go.sum
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the source code
COPY . .

# Build argument for the service path (e.g., cmd/User/api)
ARG SERVICE_PATH
RUN if [ -z "$SERVICE_PATH" ]; then echo "SERVICE_PATH is required" && exit 1; fi

# Build the binary
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-s -w" -o /app/bin/server ./${SERVICE_PATH}

# Final stage
FROM alpine:latest

# Set timezone and install ca-certificates
RUN apk update --no-cache && apk add --no-cache tzdata ca-certificates

WORKDIR /app

# Copy binary from builder
COPY --from=builder /app/bin/server /app/server

# Expose ports (can be overridden in docker-compose.yml)
# Common ports: API 8888+, RPC 9999+, Websocket 8080
EXPOSE 8080 8888 9999

# Set command
ENTRYPOINT ["/app/server"]
