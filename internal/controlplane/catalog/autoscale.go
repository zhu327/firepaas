package catalog

// autoscale.go：ADR-0041 并发信号线格式（单定义，双端共享）。
//
// autoscale:{hostname} 哈希中单个 edge field 的 JSON 由 edge reporter 写、
// controller 聚合读。两端（internal/edge、internal/controlplane/controller）
// 均以别名引用本类型，字段改名/改 tag 会同时影响编解码两侧；字段名另有
// TestAutoscaleSignalFieldNames 契约测试锁定（P2-5）。
type SignalSample struct {
	EWMA         float64 `json:"ewma"`
	RPS          float64 `json:"rps"`
	HardRejected int64   `json:"hard_rejected"`
	Unserved     int64   `json:"unserved"`
	TsMs         int64   `json:"ts_ms"`
	WinMs        int64   `json:"win_ms"`
}
