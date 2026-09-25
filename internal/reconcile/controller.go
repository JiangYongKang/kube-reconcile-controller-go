// Package reconcile 实现幂等、可重入的目标状态协调控制器。
package reconcile

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/internal/store"
)

// Result 是单次协调的结果。
type Result struct {
	Requeue bool // 是否需要再次协调
}

// Reconciler 是业务协调逻辑：对照期望状态，通过 Context.ApplyEffect
// 执行幂等副作用，并通过 Context.SetStatus 汇报状态。
type Reconciler interface {
	Reconcile(ctx context.Context, c *Context) (Result, error)
}

// Context 向 Reconciler 暴露当前对象快照与受控副作用入口。
type Context struct {
	Object *store.Object

	ctrl        *Controller
	statusPatch map[string]any
}

// ApplyEffect 以幂等方式执行副作用：
// 同一 (Key, Generation, Action) 完成过一次后，重复投递/重启重放均跳过；
// 若上次尝试中断在 claimed 状态，则续跑 fn（fn 需业务幂等）。
func (c *Context) ApplyEffect(ctx context.Context, action, detail string, fn func() error) error {
	e := Effect{
		Key:        c.Object.Key,
		Generation: c.Object.Generation,
		Action:     action,
		Detail:     detail,
	}
	done, err := c.ctrl.effects.Done(ctx, e.Key, e.Generation, e.Action)
	if err != nil {
		return fmt.Errorf("check effect: %w", err)
	}
	if done {
		return nil // 幂等短路：已生效，不再重复
	}
	if _, err := c.ctrl.effects.Claim(ctx, e); err != nil {
		return fmt.Errorf("claim effect: %w", err)
	}
	if err := fn(); err != nil {
		// 中断：记录停留在 claimed，下次重投会续跑
		return fmt.Errorf("run effect %s: %w", action, err)
	}
	if err := c.ctrl.effects.Complete(ctx, e); err != nil {
		return fmt.Errorf("complete effect: %w", err)
	}
	return nil
}

// SetStatus 记录一处 Status 变更，协调成功后由控制器统一写回。
func (c *Context) SetStatus(_ context.Context, k string, v any) {
	c.statusPatch[k] = v
}

// Controller 是协调控制器：可重入，重复投递同一对象结果一致。
//
// 单进程内同一 key 的协调通过分段锁串行化；跨进程的互斥由
// lease 包保证（同一分片同一时刻只有一个生效实例）。因此当
// 副作用记录停留在 claimed 时，可安全判定为“上次尝试中断”，
// 由当前唯一写者续跑。
type Controller struct {
	st      store.Store
	effects EffectLog
	rec     Reconciler

	queue *dedupQueue
	locks [64]sync.Mutex // 按 key 哈希分段，串行化同 key 协调
}

// New 创建控制器。
func New(st store.Store, effects EffectLog, rec Reconciler) *Controller {
	return &Controller{
		st:      st,
		effects: effects,
		rec:     rec,
		queue:   newDedupQueue(),
	}
}

// ReconcileOnce 对单个 key 执行一次协调，可重入、可并发调用
// （同 key 调用会被分段锁串行化，结果与串行执行一致）。
func (c *Controller) ReconcileOnce(ctx context.Context, key string) (Result, error) {
	lock := &c.locks[shardOf(key)]
	lock.Lock()
	defer lock.Unlock()

	obj, err := c.st.Get(ctx, key)
	if errors.Is(err, store.ErrNotFound) {
		return Result{}, nil
	}
	if err != nil {
		return Result{}, err
	}
	rctx := &Context{Object: obj, ctrl: c, statusPatch: map[string]any{}}
	res, err := c.rec.Reconcile(ctx, rctx)
	if err != nil {
		return Result{}, err
	}
	if len(rctx.statusPatch) > 0 {
		if err := c.patchStatus(ctx, key, rctx.statusPatch); err != nil {
			return Result{}, err
		}
	}
	return res, nil
}

// patchStatus 以“读取-修改-比较交换”循环写回 Status，冲突时重读重试，
// 保证并发协调下不丢失更新。
func (c *Controller) patchStatus(ctx context.Context, key string, patch map[string]any) error {
	for range 16 {
		cur, err := c.st.Get(ctx, key)
		if errors.Is(err, store.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		next := cur.DeepCopy()
		if next.Status == nil {
			next.Status = map[string]any{}
		}
		for k, v := range patch {
			next.Status[k] = v
		}
		if _, err := c.st.Update(ctx, next, cur.ResourceVersion); err != nil {
			var ce *store.ConflictError
			if errors.As(err, &ce) && ce.Reason == store.ConflictStaleVersion {
				continue // 版本被并发推进，重读重试
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("patch status for %q: too many conflicts", key)
}

func shardOf(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % 64
}

// Enqueue 投递一个 key，重复投递会被去重。
func (c *Controller) Enqueue(key string) {
	c.queue.push(key)
}

// Run 启动消费循环，直到 ctx 取消；失败的 key 延迟后自动重投。
func (c *Controller) Run(ctx context.Context) {
	for {
		key, ok := c.queue.pop(ctx)
		if !ok {
			return
		}
		res, err := c.ReconcileOnce(ctx, key)
		if err != nil || res.Requeue {
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Millisecond):
			}
			c.queue.push(key)
		}
	}
}

// dedupQueue 是一个去重队列：同一 key 在队列中只存在一份。
type dedupQueue struct {
	mu      sync.Mutex
	pending map[string]struct{}
	order   []string
}

func newDedupQueue() *dedupQueue {
	return &dedupQueue{pending: map[string]struct{}{}}
}

func (q *dedupQueue) push(key string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.pending[key]; ok {
		return // 重复投递去重
	}
	q.pending[key] = struct{}{}
	q.order = append(q.order, key)
}

func (q *dedupQueue) pop(ctx context.Context) (string, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	for len(q.order) == 0 {
		if ctx.Err() != nil {
			return "", false
		}
		// 带超时的等待，以便响应 ctx 取消
		q.mu.Unlock()
		select {
		case <-ctx.Done():
			return "", false
		case <-time.After(20 * time.Millisecond):
		}
		q.mu.Lock()
	}
	key := q.order[0]
	q.order = q.order[1:]
	delete(q.pending, key)
	return key, true
}
