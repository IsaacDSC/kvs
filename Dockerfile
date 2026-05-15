# Build
FROM golang:1.26-alpine AS build
WORKDIR /src
RUN apk add --no-cache git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/kvs-node ./cmd/node

# Runtime: make run1/run2/run3 com KVS_NODE_BIN (ver docker-compose.yml).
FROM alpine:3.21
RUN apk add --no-cache ca-certificates curl make
WORKDIR /app
COPY --from=build /out/kvs-node /app/kvs-node
COPY Makefile /app/Makefile
ENV KVS_NODE_BIN=/app/kvs-node
EXPOSE 8001 8002 8003 9001 9002 9003
