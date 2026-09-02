package edge

import (
	"container/list"
	"strings"
)

// lruCache[V] 是 edge 各本地缓存共享的容量有界 LRU 原语：RouteCache、
// RateLimiter、TokenClient 复用同一实现（P1-16 统一 container/list 版；
// F：TokenClient entries 同样有界化，否则死 machine 的凭证条目无限驻留）。
//
// 容量上限不内置默认：各调用方的默认值与 env 覆盖不同，由调用方在写入后
// 经 evict(max) 传入已解析的上限。
// 非并发安全：调用方须在自身互斥锁内使用。
type lruCache[V any] struct {
	entries map[string]*list.Element
	lru     *list.List // front = 最近使用；元素为 lruEntry[V]
}

// lruEntry 是 lru 元素值（key 冗余保存，便于淘汰时回删 map）。
type lruEntry[V any] struct {
	key   string
	value V
}

func newLRUCache[V any]() *lruCache[V] {
	return &lruCache[V]{entries: map[string]*list.Element{}, lru: list.New()}
}

// get 查找但不改变 LRU 顺序；命中后是否提升为最近使用由调用方经 touch
// 决定（RouteCache 只在真正服务请求时提升，判定前不盲目提升）。
func (c *lruCache[V]) get(key string) (V, bool) {
	var zero V
	el, ok := c.entries[key]
	if !ok {
		return zero, false
	}
	return el.Value.(lruEntry[V]).value, true
}

// touch 把 key 提到最近使用端；不存在时无操作。
func (c *lruCache[V]) touch(key string) {
	if el, ok := c.entries[key]; ok {
		c.lru.MoveToFront(el)
	}
}

// set 写入/更新条目并提到最近使用端；不做淘汰（调用方随后 evict）。
func (c *lruCache[V]) set(key string, v V) {
	if el, ok := c.entries[key]; ok {
		el.Value = lruEntry[V]{key: key, value: v}
		c.lru.MoveToFront(el)
		return
	}
	c.entries[key] = c.lru.PushFront(lruEntry[V]{key: key, value: v})
}

// evict 从最久未用端淘汰，直到条目数 <= max。调用方必须传入已解析为
// 正数的上限（<=0 视为调用方 bug，按 1 处理，绝不放开为无界）。
func (c *lruCache[V]) evict(max int) {
	if max <= 0 {
		max = 1
	}
	for c.lru.Len() > max {
		back := c.lru.Back()
		if back == nil {
			break
		}
		delete(c.entries, back.Value.(lruEntry[V]).key)
		c.lru.Remove(back)
	}
}

func (c *lruCache[V]) delete(key string) {
	if el, ok := c.entries[key]; ok {
		delete(c.entries, key)
		c.lru.Remove(el)
	}
}

// deletePrefix 删除所有匹配前缀的 key（TokenClient.Invalidate 按 machine
// 前缀删除其全部 execution 的条目；调用方锁内使用）。
func (c *lruCache[V]) deletePrefix(prefix string) {
	for k := range c.entries {
		if strings.HasPrefix(k, prefix) {
			c.delete(k)
		}
	}
}

func (c *lruCache[V]) len() int { return len(c.entries) }
