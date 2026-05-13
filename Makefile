.PHONY: help proto build run-single-node run1 run2 run3 state1 state2 state3 propose

# Alvo padrão: exibe a ajuda
.DEFAULT_GOAL := help

# protoc vem do sistema: macOS: brew install protobuf; Debian/Ubuntu: apt install protobuf-compiler
# Garante Homebrew, /usr/local e bin do Go (protoc-gen-go) no PATH.
GOPATH := $(shell go env GOPATH 2>/dev/null)
export PATH := /opt/homebrew/bin:/usr/local/bin:$(GOPATH)/bin:$(PATH)
PROTOC ?= protoc

# Intervalo do checkpoint WAL nos alvos run-* (0 desliga o ticker; default 1m).
CHECKPOINT_INTERVAL ?= 1m

# Limite LRU por tabela na memdb nos alvos run-* (default do binário: 1000; 0 = ilimitado).
MEMDB_MAX_ENTRIES ?= 2000

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

build:
	go build ./...

# Single node leader
run-single-node: build
	go run ./cmd/node/main.go -id node1 \
		-grpc-addr :9001 -http-addr :8001 \
		-peers "" \
		-checkpoint-interval $(CHECKPOINT_INTERVAL) \
		-memdb-max-entries $(MEMDB_MAX_ENTRIES)

# ── Cluster (open one terminal per target) ─────────────────────────────────
# Node-to-node RPCs use gRPC on ports 9001/9002/9003.
# Client API (curl) uses HTTP on ports 8001/8002/8003.

run1:
	go run ./cmd/node/main.go -id node1 \
		-grpc-addr :9001 -http-addr :8001 \
		-peers localhost:9002,localhost:9003 \
		-checkpoint-interval $(CHECKPOINT_INTERVAL) \
		-memdb-max-entries $(MEMDB_MAX_ENTRIES)

run2:
	go run ./cmd/node/main.go -id node2 \
		-grpc-addr :9002 -http-addr :8002 \
		-peers localhost:9001,localhost:9003 \
		-checkpoint-interval $(CHECKPOINT_INTERVAL) \
		-memdb-max-entries $(MEMDB_MAX_ENTRIES)

run3:
	go run ./cmd/node/main.go -id node3 \
		-grpc-addr :9003 -http-addr :8003 \
		-peers localhost:9001,localhost:9002 \
		-checkpoint-interval $(CHECKPOINT_INTERVAL) \
		-memdb-max-entries $(MEMDB_MAX_ENTRIES)

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
	@echo "  make build          - Compila o projeto (go build ./...)"
	@echo "  make run-single-node - Sobe 1 nó (HTTP :8001 / gRPC :9001)"
	@echo "  make run1           - Sobe o node1 (HTTP :8001 / gRPC :9001)"
	@echo "  make run2           - Sobe o node2 (HTTP :8002 / gRPC :9002)"
	@echo "  make run3           - Sobe o node3 (HTTP :8003 / gRPC :9003)"
	@echo "  CHECKPOINT_INTERVAL=0 make run1  - desliga checkpoint WAL periódico (default 1m no Makefile)"
	@echo "  MEMDB_MAX_ENTRIES=5000 make run1  - limite LRU memdb por tabela (default 2000 no Makefile; binário default 1000)"
	@echo "  make state1         - Mostra /state do node1 (requer jq)"
	@echo "  make state2         - Mostra /state do node2 (requer jq)"
	@echo "  make state3         - Mostra /state do node3 (requer jq)"
	@echo "  make propose        - POST /cmd/propose (ADDR=... CMD=...)"
	@echo "  make proto  - Gera código Go e gRPC a partir de proto/raft/raft.proto"
	@echo "  make help   - Mostra esta ajuda (é o alvo padrão ao rodar 'make' sem argumentos)"
