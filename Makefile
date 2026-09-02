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

check: build vet test tidy-check ## CI 本地等价入口：build+vet+test+tidy 检查（不含 PG/Redis-gated 测试）

check-lab: build vet ## 实验室全量检查：含需要 PG/Redis 的测试（make dev-up 后执行；P2-5）
	FIREPAAS_TEST_POSTGRES='postgres://firepaas:firepaas@127.0.0.1:5432/firepaas?sslmode=disable' \
		FIREPAAS_TEST_REDIS=127.0.0.1:6379 \
		go test -count=1 ./...
	go run ./tools/sim -n 100000

tidy-check: ## 验证 go.mod/go.sum 已 tidy（不改工作树）
	@tmp=$$(mktemp -d); trap 'rm -rf "$$tmp"' EXIT; \
	  cp go.mod "$$tmp/check.mod"; cp go.sum "$$tmp/check.sum"; \
	  GOWORK=off go mod tidy -modfile="$$tmp/check.mod"; \
	  diff -u go.mod "$$tmp/check.mod"; \
	  diff -u go.sum "$$tmp/check.sum" || { echo "go.mod/go.sum not tidy: run 'go mod tidy' and commit"; exit 1; }

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

sim: ## M2.6 调度仿真：10 万次放置断言（过滤先于打分/硬准入/反亲和/失联排除）
	go run ./tools/sim -n 100000

dev-up: ## 启动本地开发依赖(postgres/redis/minio,需要 docker)
	docker compose -f iac/dev/docker-compose.yaml up -d

clean: ## 仅清理未跟踪产物（bin/）；shared/gen/ 是已跟踪的 protobuf 生成物，不再删除
	rm -rf bin/

# --- 生产就绪 P2#19：控制面/edge 生产镜像（与 CI images job 同一 Dockerfile；
# agentd 不走镜像，raw_exec 要求宿主 root/cgroup/netns，见 Dockerfile.api 头注） ---

.PHONY: images
images: ## 构建 api/edge 生产镜像（本地 tag firepaas-api:local / firepaas-edge-proxy:local）
	docker build -f Dockerfile.api -t firepaas-api:local .
	docker build -f Dockerfile.edge-proxy -t firepaas-edge-proxy:local .
