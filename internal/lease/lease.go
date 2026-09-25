// Package lease 实现基于对象存储的分片租约，用于多实例下互斥协调。
//
// 互斥原理：租约是存储中的一个普通对象，所有获取/接管都通过
// 乐观并发（携带 ResourceVersion 的 CAS 更新）完成，
// 因此同一时刻只有一个实例能成功推进租约，杜绝双主。
// 每次租约易主，Fencing 令牌单调递增，下游可据此拒绝过期持有者。
package lease

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/internal/store"
)

// ErrHeldByOther 表示租约被其他实例持有且未过期。
var ErrHeldByOther = errors.New("lease held by other instance")

// ErrLostLease 表示当前实例不持有租约（从未持有、已过期或被接管）。
var ErrLostLease = errors.New("lease lost")

// Lease 描述一个分片租约。
type Lease struct {
	Shard     string
	Holder    string
	Fencing   int64 // 围栏令牌，随租约易主单调递增
	ExpiresAt time.Time
}

func leaseKey(shard string) string { return "__lease__/" + shard }

// Manager 管理分片租约。
type Manager struct {
	st  store.Store
	now func() time.Time
}

// NewManager 创建租约管理器。now 可注入以便测试租约过期。
func NewManager(st store.Store, now func() time.Time) *Manager {
	if now == nil {
		now = time.Now
	}
	return &Manager{st: st, now: now}
}

const maxRetries = 16

// Acquire 尝试获取分片租约：
//   - 租约不存在：创建之（Fencing 从 1 开始）；
//   - 自己持有：等价于续租；
//   - 他人持有且已过期：接管，Fencing+1；
//   - 他人持有且未过期：返回 ErrHeldByOther。
func (m *Manager) Acquire(ctx context.Context, shard, holder string, ttl time.Duration) (*Lease, error) {
	for range maxRetries {
		obj, err := m.st.Get(ctx, leaseKey(shard))
		if errors.Is(err, store.ErrNotFound) {
			l := &Lease{Shard: shard, Holder: holder, Fencing: 1, ExpiresAt: m.now().Add(ttl)}
			if _, err := m.st.Create(ctx, leaseToObject(l)); err != nil {
				var ce *store.ConflictError
				if errors.As(err, &ce) && ce.Reason == store.ConflictAlreadyExists {
					continue // 并发创建，重读后走 CAS 路径
				}
				return nil, err
			}
			return l, nil
		}
		if err != nil {
			return nil, err
		}
		cur := objectToLease(shard, obj)
		switch {
		case cur.Holder == holder && m.now().Before(cur.ExpiresAt):
			// 自己持有，续期
			cur.ExpiresAt = m.now().Add(ttl)
		case m.now().Before(cur.ExpiresAt):
			// 他人持有且未过期
			return nil, fmt.Errorf("%w: shard=%q holder=%q", ErrHeldByOther, shard, cur.Holder)
		default:
			// 已过期（含自己持有但过期）：接管/重取，Fencing+1
			cur.Holder = holder
			cur.Fencing++
			cur.ExpiresAt = m.now().Add(ttl)
		}
		next := leaseToObject(cur)
		if _, err := m.st.Update(ctx, next, obj.ResourceVersion); err != nil {
			var ce *store.ConflictError
			if errors.As(err, &ce) && ce.Reason == store.ConflictStaleVersion {
				continue // 并发竞争，重读重试
			}
			return nil, err
		}
		return cur, nil
	}
	return nil, fmt.Errorf("acquire lease for shard %q: too many conflicts", shard)
}

// Renew 续租；未持有租约（已易主或过期被接管）时返回 ErrLostLease。
func (m *Manager) Renew(ctx context.Context, shard, holder string, ttl time.Duration) (*Lease, error) {
	for range maxRetries {
		obj, err := m.st.Get(ctx, leaseKey(shard))
		if errors.Is(err, store.ErrNotFound) {
			return nil, fmt.Errorf("%w: shard=%q not held", ErrLostLease, shard)
		}
		if err != nil {
			return nil, err
		}
		cur := objectToLease(shard, obj)
		if cur.Holder != holder || !m.now().Before(cur.ExpiresAt) {
			return nil, fmt.Errorf("%w: shard=%q heldBy=%q", ErrLostLease, shard, cur.Holder)
		}
		cur.ExpiresAt = m.now().Add(ttl)
		next := leaseToObject(cur)
		if _, err := m.st.Update(ctx, next, obj.ResourceVersion); err != nil {
			var ce *store.ConflictError
			if errors.As(err, &ce) && ce.Reason == store.ConflictStaleVersion {
				continue
			}
			return nil, err
		}
		return cur, nil
	}
	return nil, fmt.Errorf("renew lease for shard %q: too many conflicts", shard)
}

// Release 主动释放租约（立即使其过期，保留 Fencing 令牌）；未持有返回 ErrLostLease。
func (m *Manager) Release(ctx context.Context, shard, holder string) error {
	obj, err := m.st.Get(ctx, leaseKey(shard))
	if errors.Is(err, store.ErrNotFound) {
		return fmt.Errorf("%w: shard=%q not held", ErrLostLease, shard)
	}
	if err != nil {
		return err
	}
	cur := objectToLease(shard, obj)
	if cur.Holder != holder {
		return fmt.Errorf("%w: shard=%q heldBy=%q", ErrLostLease, shard, cur.Holder)
	}
	cur.ExpiresAt = m.now() // 立即过期，允许他人接管
	next := leaseToObject(cur)
	_, err = m.st.Update(ctx, next, obj.ResourceVersion)
	return err
}

// Current 读取当前租约，不存在返回 (nil, nil)。
func (m *Manager) Current(ctx context.Context, shard string) (*Lease, error) {
	obj, err := m.st.Get(ctx, leaseKey(shard))
	if errors.Is(err, store.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return objectToLease(shard, obj), nil
}

// CheckHeld 校验 holder 是否仍持有未过期租约且 Fencing 令牌匹配，
// 用于副作用执行前的防双主检查。
func (m *Manager) CheckHeld(ctx context.Context, shard, holder string, fencing int64) error {
	cur, err := m.Current(ctx, shard)
	if err != nil {
		return err
	}
	if cur == nil || cur.Holder != holder || cur.Fencing != fencing || !m.now().Before(cur.ExpiresAt) {
		return fmt.Errorf("%w: shard=%q holder=%q fencing=%d", ErrLostLease, shard, holder, fencing)
	}
	return nil
}

func leaseToObject(l *Lease) *store.Object {
	return &store.Object{
		Key: leaseKey(l.Shard),
		Status: map[string]any{
			"holder":    l.Holder,
			"fencing":   l.Fencing,
			"expiresAt": l.ExpiresAt.UnixNano(),
		},
	}
}

func objectToLease(shard string, obj *store.Object) *Lease {
	l := &Lease{Shard: shard}
	if h, ok := obj.Status["holder"].(string); ok {
		l.Holder = h
	}
	if f, ok := obj.Status["fencing"].(int64); ok {
		l.Fencing = f
	}
	if e, ok := obj.Status["expiresAt"].(int64); ok {
		l.ExpiresAt = time.Unix(0, e)
	}
	return l
}
