.PHONY: help proto

# Alvo padrão: exibe a ajuda
.DEFAULT_GOAL := help

# protoc vem do sistema: macOS: brew install protobuf; Debian/Ubuntu: apt install protobuf-compiler
# Garante Homebrew, /usr/local e bin do Go (protoc-gen-go) no PATH.
GOPATH := $(shell go env GOPATH 2>/dev/null)
export PATH := /opt/homebrew/bin:/usr/local/bin:$(GOPATH)/bin:$(PATH)
PROTOC ?= protoc

proto:
	@command -v $(PROTOC) >/dev/null 2>&1 || { \
		echo "erro: 'protoc' não encontrado. Instale o Protocol Buffers, por ex.: brew install protobuf"; \
		exit 1; \
	}
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	$(PROTOC) --go_out=. --go_opt=paths=source_relative \
		--go-grpc_out=. --go-grpc_opt=paths=source_relative \
		proto/raft/raft.proto

# ── Cluster (open one terminal per target) ─────────────────────────────────
# Node-to-node RPCs use gRPC on ports 9001/9002/9003.
# Client API (curl) uses HTTP on ports 8001/8002/8003.

run1: 
	go run ./cmd/node/main.go -id node1 \
	           -grpc-addr :9001 -http-addr :8001 \
	           -peers localhost:9002,localhost:9003

run2: 
	go run ./cmd/node/main.go -id node2 \
	           -grpc-addr :9002 -http-addr :8002 \
	           -peers localhost:9001,localhost:9003

run3: 
	go run ./cmd/node/main.go -id node3 \
	           -grpc-addr :9003 -http-addr :8003 \
	           -peers localhost:9001,localhost:9002

# ── Inspect state ───────────────────────────────────────────────────────────

state1:
	curl -s localhost:8001/state | jq .

state2:
	curl -s localhost:8002/state | jq .

state3:
	curl -s localhost:8003/state | jq .

# ── Propose a command ───────────────────────────────────────────────────────
# Usage: make propose ADDR=localhost:8001 CMD='set x 42'

propose:
	@test -n "$(ADDR)" || { echo "erro: informe ADDR, ex.: ADDR=localhost:8001"; exit 2; }
	@url="$(ADDR)"; \
	case "$$url" in \
		http://*|https://*) ;; \
		*) url="http://$$url" ;; \
	esac; \
	curl -s -X POST "$$url/cmd/propose" \
		-H 'Content-Type: application/json' \
		-d '{"command":"$(CMD)"}'

help:
	@echo "Comandos disponíveis:"
	@echo "  make proto  - Gera código Go e gRPC a partir de proto/raft/raft.proto"
	@echo "  make help   - Mostra esta ajuda (é o alvo padrão ao rodar 'make' sem argumentos)"
