# firepaas 顶层 Makefile(P0-P1 逐步补全,当前为骨架)

SHELL := /bin/bash

.PHONY: help build test lint proto dev-up clean

help: ## 列出可用目标
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## 构建全部组件
	cd agent && go build ./...
	cd control-plane && go build ./...
	cd edge && go build ./...
	cd shared && go build ./...

test: ## 运行全部测试
	cd shared && go test ./...
	cd agent && go test ./...
	cd control-plane && go test ./...
	cd edge && go test ./...

lint: ## 静态检查(逐模块执行,需要 golangci-lint)
	@command -v golangci-lint >/dev/null || { echo "golangci-lint not installed"; exit 1; }
	@for d in agent control-plane edge shared; do \
		echo "==> lint $$d"; (cd $$d && golangci-lint run ./...); \
	done

proto: ## 生成 protobuf 代码(需要 protoc + protoc-gen-go + protoc-gen-go-grpc)
	@echo "ERROR: proto 目标尚未接入(M1.2 契约冻结后启用,禁止 no-op target,见 mvp-plan §5.1)" >&2
	@exit 1

dev-up: ## 启动本地开发依赖(postgres/redis/minio,需要 docker)
	docker compose -f iac/dev/docker-compose.yaml up -d

clean:
	rm -rf bin/ shared/gen/
