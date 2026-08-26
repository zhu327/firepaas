// Package server 是 agent gRPC 服务实现(P1 接线,当前为占位)。
package server

// Server 聚合 InfoService / MachineService / ImageService。
// 依赖方向:server -> machine / image / network / info,反向禁止。
type Server struct {
	// TODO(P1.3): MachineManager、ImageManager、InfoProvider、SlotManager
}
