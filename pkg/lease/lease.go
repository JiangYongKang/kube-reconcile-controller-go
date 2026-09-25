// Package lease 提供分片租约，保证同一分片任一时刻只有一个实例生效。
package lease

import (
	"errors"
	"sync"
	"time"
)

// ErrHeldByOther 表示租约被其他实例持有且未过期。
var ErrHeldByOther = errors.New("lease held by other instance")

// Record 是租约的持久化记录。
type Record struct {
	Shard     string
	Holder    string
	ExpiresAt time.Time
	// Version 用于租约记录自身的乐观并发控制。
	Version int64
}

// Clock 抽象时间来源，便于测试。
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// FakeClock 是可手动推进的时钟，用于测试租约过期与接管。
type FakeClock struct {
	mu  sync.Mutex
	now time.Time
}

// NewFakeClock 创建起始时间为 t 的假时钟。
func NewFakeClock(t time.Time) *FakeClock { return &FakeClock{now: t} }

// Now 返回当前假时间。
func (f *FakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

// Advance 将假时钟推进 d。
func (f *FakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

// Manager 管理一组分片租约，基于 Store 风格的内存实现。
type Manager struct {
	mu     sync.Mutex
	clock  Clock
	ttl    time.Duration
	leases map[string]*Record
}

// NewManager 创建租约管理器，ttl 为租约有效期。
func NewManager(ttl time.Duration) *Manager {
	return &Manager{clock: realClock{}, ttl: ttl, leases: make(map[string]*Record)}
}

// SetClock 替换时钟来源（测试用）。
func (m *Manager) SetClock(c Clock) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.clock = c
}

// Acquire 尝试获取或续租分片租约。
// 成功返回当前租约记录；被他人持有且未过期返回 ErrHeldByOther。
// 规则：
//   - 无租约或租约已过期：任何实例可获取/接管；
//   - 持有者本人：续租，延长过期时间；
//   - 他人持有且未过期：拒绝，防止双主。
func (m *Manager) Acquire(shard, holder string) (*Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock.Now()
	cur, ok := m.leases[shard]
	if ok && cur.Holder != holder && now.Before(cur.ExpiresAt) {
		return nil, ErrHeldByOther
	}
	rec := &Record{
		Shard:     shard,
		Holder:    holder,
		ExpiresAt: now.Add(m.ttl),
	}
	if ok {
		rec.Version = cur.Version + 1
	} else {
		rec.Version = 1
	}
	m.leases[shard] = rec
	cp := *rec
	return &cp, nil
}

// HeldBy 判断分片当前是否由 holder 有效持有。
func (m *Manager) HeldBy(shard, holder string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	cur, ok := m.leases[shard]
	return ok && cur.Holder == holder && m.clock.Now().Before(cur.ExpiresAt)
}

// Release 主动释放租约（仅持有者可释放）。
func (m *Manager) Release(shard, holder string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if cur, ok := m.leases[shard]; ok && cur.Holder == holder {
		delete(m.leases, shard)
	}
}

// Snapshot 返回租约记录副本，用于日志与测试断言。
func (m *Manager) Snapshot(shard string) (Record, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.leases[shard]
	if !ok {
		return Record{}, false
	}
	return *r, true
}
