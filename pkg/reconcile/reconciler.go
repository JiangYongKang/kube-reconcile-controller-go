// Package reconcile 实现幂等、可重入的目标状态协调器。
package reconcile

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/store"
)

// Handler 是业务协调逻辑：将对象调整到目标状态。
// 实现必须幂等：对同一对象重复调用不得产生重复副作用。
type Handler interface {
	// Reconcile 处理单个对象，返回需要回写的 Status（可为 nil）。
	Reconcile(ctx context.Context, obj *store.Object) (map[string]string, error)
}

// Queue 是去重工作队列：同一 key 在队列中只保留一份，
// 重复投递不会导致重复协调。
type Queue struct {
	mu     sync.Mutex
	items  map[string]struct{}
	order  []string
	notify chan struct{} // 容量 1 的信号通道，用于唤醒等待者
	closed bool
}

// NewQueue 创建工作队列。
func NewQueue() *Queue {
	return &Queue{
		items:  make(map[string]struct{}),
		notify: make(chan struct{}, 1),
	}
}

// Add 投递 key；已存在则去重忽略。
func (q *Queue) Add(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return
	}
	if _, dup := q.items[key]; dup {
		return
	}
	q.items[key] = struct{}{}
	q.order = append(q.order, key)
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Get 取出一个 key；队列为空时阻塞等待，直到有元素、
// ctx 取消或队列关闭（后两种情况返回 ok=false）。
func (q *Queue) Get(ctx context.Context) (key string, ok bool) {
	for {
		q.mu.Lock()
		if len(q.order) > 0 {
			key = q.order[0]
			q.order = q.order[1:]
			delete(q.items, key)
			q.mu.Unlock()
			return key, true
		}
		if q.closed {
			q.mu.Unlock()
			return "", false
		}
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", false
		case <-q.notify:
		}
	}
}

// Len 返回队列中待处理的 key 数量。
func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.order)
}

// Close 关闭队列，唤醒所有等待者。
func (q *Queue) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.closed = true
	select {
	case q.notify <- struct{}{}:
	default:
	}
}

// Controller 是协调控制器：从队列取对象，在持有分片租约的前提下
// 调用 Handler 并乐观并发回写状态。
type Controller struct {
	Store     *store.MemoryStore
	Leases    *lease.Manager
	Handler   Handler
	Queue     *Queue
	Instance  string // 实例 ID，用于租约持有
	Logger    *log.Logger
}

// NewController 创建控制器。
func NewController(instance string, st *store.MemoryStore, lm *lease.Manager, h Handler, logger *log.Logger) *Controller {
	return &Controller{
		Store:    st,
		Leases:   lm,
		Handler:  h,
		Queue:    NewQueue(),
		Instance: instance,
		Logger:   logger,
	}
}

// ReconcileOne 对单个 key 执行一次协调，返回是否需要重试。
// 流程：检查租约 -> 读取对象 -> 调用 Handler -> 乐观并发回写。
// 幂等保证：Handler 返回的期望状态与当前状态一致时不写回，
// 重复投递/重放不会产生新的版本或重复副作用。
func (c *Controller) ReconcileOne(ctx context.Context, shard, key string) (requeue bool, err error) {
	// 1. 租约检查：未持有则尝试获取/接管；被他人持有则稍后重试。
	if !c.Leases.HeldBy(shard, c.Instance) {
		rec, aerr := c.Leases.Acquire(shard, c.Instance)
		if aerr != nil {
			c.logf("key=%s shard=%s 判定=租约被他人持有 依据=%v -> 稍后重试", key, shard, aerr)
			return true, nil
		}
		c.logf("key=%s shard=%s 判定=获得租约 holder=%s leaseVersion=%d expiresAt=%s",
			key, shard, rec.Holder, rec.Version, rec.ExpiresAt.Format("15:04:05.000"))
	}

	// 2. 读取对象当前版本。
	obj, err := c.Store.Get(key)
	if err != nil {
		return false, err
	}
	if obj == nil {
		c.logf("key=%s shard=%s 判定=对象已删除 -> 无需协调", key, shard)
		return false, nil
	}
	c.logf("key=%s shard=%s 读取 rv=%d gen=%d", key, shard, obj.ResourceVersion, obj.Generation)

	// 3. 调用业务 Handler 计算期望状态。
	desired, herr := c.Handler.Reconcile(ctx, obj)
	if herr != nil {
		c.logf("key=%s shard=%s rv=%d 判定=Handler失败 依据=%v -> 重试", key, shard, obj.ResourceVersion, herr)
		return true, herr
	}
	if desired == nil || equalMap(desired, obj.Status) {
		c.logf("key=%s shard=%s rv=%d 判定=已达目标状态 -> 幂等跳过写回", key, shard, obj.ResourceVersion)
		return false, nil
	}

	// 4. 乐观并发回写：携带读取时的 rv，冲突则重读重试，不丢更新。
	obj.Status = desired
	updated, uerr := c.Store.Update(obj)
	if uerr != nil {
		if reason, ok := store.IsConflict(uerr); ok {
			c.logf("key=%s shard=%s rv=%d 判定=写回冲突 原因=%s -> 重读重试", key, shard, obj.ResourceVersion, reason)
			return true, nil
		}
		return true, uerr
	}
	c.logf("key=%s shard=%s 判定=写回成功 rv=%d->%d", key, shard, obj.ResourceVersion, updated.ResourceVersion)
	return false, nil
}

// Run 启动协调循环，直到 ctx 取消或队列关闭。
func (c *Controller) Run(ctx context.Context, shard string) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		key, ok := c.Queue.Get(ctx)
		if !ok {
			return
		}
		requeue, err := c.ReconcileOne(ctx, shard, key)
		if err != nil {
			c.logf("key=%s shard=%s 协调出错: %v", key, shard, err)
		}
		if requeue {
			// 简单退避后重新入队，避免热循环。
			select {
			case <-ctx.Done():
				return
			case <-time.After(50 * time.Millisecond):
			}
			c.Queue.Add(key)
		}
	}
}

func (c *Controller) logf(format string, args ...any) {
	if c.Logger != nil {
		c.Logger.Printf("instance=%s "+format, append([]any{c.Instance}, args...)...)
	}
}

func equalMap(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}
