// Package e2e 端到端验证：多实例 + 租约互斥 + 幂等协调，
// 覆盖实例故障接管后最终状态可预期。
package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/internal/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/internal/reconcile"
	"github.com/highcumontoa/kube-reconcile-controller-go/internal/store"
)

// clock 可手动推进的时钟。
type clock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *clock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *clock) Advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }

// effectCounter 记录全局副作用生效次数（跨实例共享，模拟外部系统）。
type effectCounter struct {
	mu sync.Mutex
	n  map[string]int
}

func (e *effectCounter) incr(action string, gen int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.n[fmt.Sprintf("%s@%d", action, gen)]++
}

func (e *effectCounter) get(action string, gen int64) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.n[fmt.Sprintf("%s@%d", action, gen)]
}

// instance 模拟一个控制器实例：先抢租约，持约才协调，
// 执行副作用前用 Fencing 令牌做防双主检查。
type instance struct {
	id      string
	lm      *lease.Manager
	ctrl    *reconcile.Controller
	counter *effectCounter
}

func newInstance(id string, st store.Store, lm *lease.Manager, counter *effectCounter) *instance {
	inst := &instance{id: id, lm: lm, counter: counter}
	inst.ctrl = reconcile.New(st, reconcile.NewStoreEffectLog(st), reconcilerFunc(func(ctx context.Context, c *reconcile.Context) (reconcile.Result, error) {
		err := c.ApplyEffect(ctx, "ensure-workload", "apply desired state", func() error {
			counter.incr("ensure-workload", c.Object.Generation)
			return nil
		})
		if err != nil {
			return reconcile.Result{}, err
		}
		c.SetStatus(ctx, "phase", "Ready")
		c.SetStatus(ctx, "reconciledBy", id)
		return reconcile.Result{}, nil
	}))
	return inst
}

type reconcilerFunc func(ctx context.Context, c *reconcile.Context) (reconcile.Result, error)

func (f reconcilerFunc) Reconcile(ctx context.Context, c *reconcile.Context) (reconcile.Result, error) {
	return f(ctx, c)
}

// tryReconcile 抢租约并在持约期间协调；未持约返回 false。
func (i *instance) tryReconcile(ctx context.Context, shard, key string, ttl time.Duration) (bool, error) {
	l, err := i.lm.Acquire(ctx, shard, i.id, ttl)
	if errors.Is(err, lease.ErrHeldByOther) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	// 防双主：协调前校验 Fencing 令牌仍然有效
	if err := i.lm.CheckHeld(ctx, shard, i.id, l.Fencing); err != nil {
		return false, err
	}
	if _, err := i.ctrl.ReconcileOnce(ctx, key); err != nil {
		return false, err
	}
	return true, nil
}

// 故障接管端到端：inst-1 生效 → 失联 → inst-2 接管，
// 全程副作用只生效一次，最终状态收敛且可预期。
func TestFailoverConverges(t *testing.T) {
	ctx := context.Background()
	st := store.NewMemoryStore()
	clk := &clock{now: time.Unix(1_700_000_000, 0)}
	lm := lease.NewManager(st, clk.Now)
	counter := &effectCounter{n: map[string]int{}}
	ttl := 30 * time.Second

	if _, err := st.Create(ctx, &store.Object{Key: "app/web", Spec: map[string]any{"replicas": 3}}); err != nil {
		t.Fatal(err)
	}

	inst1 := newInstance("inst-1", st, lm, counter)
	inst2 := newInstance("inst-2", st, lm, counter)

	// inst-1 持约协调；inst-2 不得生效
	ok1, err := inst1.tryReconcile(ctx, "shard-0", "app/web", ttl)
	if err != nil || !ok1 {
		t.Fatalf("inst-1 should hold lease and reconcile: ok=%v err=%v", ok1, err)
	}
	ok2, err := inst2.tryReconcile(ctx, "shard-0", "app/web", ttl)
	if err != nil || ok2 {
		t.Fatalf("inst-2 must not reconcile while inst-1 holds lease: ok=%v err=%v", ok2, err)
	}
	obj, _ := st.Get(ctx, "app/web")
	t.Logf("inst-1 active: rv=%d phase=%v by=%v effects=%d (判定: 未持约实例不得协调)",
		obj.ResourceVersion, obj.Status["phase"], obj.Status["reconciledBy"],
		counter.get("ensure-workload", 1))

	// inst-1 失联：时钟推进超过 TTL
	clk.Advance(ttl + time.Second)

	// inst-2 接管并协调；副作用不得重复生效
	ok2, err = inst2.tryReconcile(ctx, "shard-0", "app/web", ttl)
	if err != nil || !ok2 {
		t.Fatalf("inst-2 should take over: ok=%v err=%v", ok2, err)
	}
	cur, _ := lm.Current(ctx, "shard-0")
	obj, _ = st.Get(ctx, "app/web")
	t.Logf("inst-2 takeover: fencing=%d rv=%d phase=%v effects=%d (判定: 接管后重放协调, 已done的副作用跳过)",
		cur.Fencing, obj.ResourceVersion, obj.Status["phase"], counter.get("ensure-workload", 1))

	if counter.get("ensure-workload", 1) != 1 {
		t.Fatalf("effect applied %d times across failover, want 1", counter.get("ensure-workload", 1))
	}
	if obj.Status["phase"] != "Ready" {
		t.Fatalf("status not converged: %v", obj.Status)
	}
	if cur.Holder != "inst-2" || cur.Fencing != 2 {
		t.Fatalf("unexpected lease after takeover: %+v", cur)
	}

	// inst-1 “复活”后不得再生效（租约已易主）
	ok1, err = inst1.tryReconcile(ctx, "shard-0", "app/web", ttl)
	t.Logf("revived inst-1: ok=%v err=%v (判定: 旧实例重新抢约前不得协调; 抢约结果取决于是否过期)", ok1, err)
	if err != nil && !errors.Is(err, lease.ErrLostLease) {
		t.Fatalf("unexpected error from revived instance: %v", err)
	}
	if counter.get("ensure-workload", 1) != 1 {
		t.Fatalf("effect count changed after revival: %d", counter.get("ensure-workload", 1))
	}
}
