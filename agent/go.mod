module github.com/example/firepaas/agent

go 1.25.4

// P1 阶段按需添加依赖:
//   github.com/kernel/hypeman  (本地 replace => ../../hypeman,先 import lib/hypervisor 与 lib/instances)
//   google.golang.org/grpc
//   google.golang.org/protobuf
