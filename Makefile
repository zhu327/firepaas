# firepaas 顶层 Makefile(P0-P1 逐步补全,当前为骨架)

SHELL := /bin/bash

.PHONY: help build test lint proto dev-up check tidy-check clean

help: ## 列出可用目标
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## 构建全部组件（根 module）
	go build ./...

test: ## 运行全部测试
	go test ./...

vet: ## go vet
	go vet ./...

lint: ## 静态检查(需要 golangci-lint)
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed"; exit 1; }
	golangci-lint run ./...

check: build vet test tidy-check ## CI 本地等价入口：build+vet+test+tidy 检查

tidy-check: ## 验证 go.mod/go.sum 已 tidy（评审 P2-7）
	go mod tidy
	@git diff --exit-code go.mod go.sum || { echo "go.mod/go.sum not tidy: run 'go mod tidy' and commit"; exit 1; }

LAB_ROOT ?= $(HOME)/.local/firepaas-lab
PROTOC ?= $(LAB_ROOT)/bin/protoc
PROTOC_INCLUDE ?= $(LAB_ROOT)/protoc/include

proto: ## 生成 protobuf 代码（需要 scripts/lab/install-protoc.sh 先执行）
	@command -v $(PROTOC) >/dev/null || { echo "protoc not found: run bash scripts/lab/install-protoc.sh"; exit 1; }
	rm -rf shared/gen
	mkdir -p shared/gen
	PATH="$(LAB_ROOT)/bin:$$PATH" $(PROTOC) \
		-I protos -I $(PROTOC_INCLUDE) \
		--go_out=shared/gen --go_opt=paths=source_relative \
		--go-grpc_out=shared/gen --go-grpc_opt=paths=source_relative \
		protos/agent/v1/agent.proto
	@echo "generated: shared/gen/agent/v1/*.pb.go"

dev-up: ## 启动本地开发依赖(postgres/redis/minio,需要 docker)
	docker compose -f iac/dev/docker-compose.yaml up -d

clean:
	rm -rf bin/ shared/gen/
