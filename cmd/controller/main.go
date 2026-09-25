// 本程序在本地演示目标状态协调控制器：
// 两个实例通过租约互斥协调同一分片，中途模拟持有者故障与接管，
// 全程打印资源版本与判定依据，最终状态可预期、可验证。
package main

import (
	"context"
	"log"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/internal/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/internal/reconcile"
	"github.com/highcumontoa/kube-reconcile-controller-go/internal/store"
)

const (
	shard = "shard-0"
	key   = "app/web"
	ttl   = 500 * time.Millisecond
)

func main() {
	ctx := context.Background()
	st := store.NewMemoryStore()
	lm := lease.NewManager(st, nil) // 真实时钟

	if _, err := st.Create(ctx, &store.Object{
		Key:  key,
		Spec: map[string]any{"replicas": 3},
	}); err != nil {
		log.Fatal(err)
	}
	log.Printf("[setup] created %s", key)

	// 实例1：持约协调两轮后“故障”（退出，不再续租）
	inst1 := newInstance("inst-1", st, lm)
	runInstance(ctx, inst1, 2)
	log.Printf("[%s] crashed (lease will expire after %s)", inst1.id, ttl)

	// 实例2：等待租约过期后接管并协调
	inst2 := newInstance("inst-2", st, lm)
	time.Sleep(ttl + 100*time.Millisecond)
	runInstance(ctx, inst2, 2)

	// 打印最终状态与副作用记录
	obj, _ := st.Get(ctx, key)
	cur, _ := lm.Current(ctx, shard)
	log.Printf("[final] obj rv=%d gen=%d phase=%v reconciledBy=%v",
		obj.ResourceVersion, obj.Generation, obj.Status["phase"], obj.Status["reconciledBy"])
	log.Printf("[final] lease holder=%s fencing=%d expiresAt=%s",
		cur.Holder, cur.Fencing, cur.ExpiresAt.Format(time.RFC3339))
	log.Printf("[final] effect ensure-workload@gen1 applied=%d times (expect 1)", effectsApplied)
	if effectsApplied != 1 {
		log.Fatal("FAIL: side effect applied more than once")
	}
	log.Print("OK: converged without duplicate effects and without dual primary")
}

var effectsApplied int // 模拟外部系统的副作用计数

type instance struct {
	id   string
	st   store.Store
	lm   *lease.Manager
	ctrl *reconcile.Controller
}

func newInstance(id string, st store.Store, lm *lease.Manager) *instance {
	inst := &instance{id: id, st: st, lm: lm}
	inst.ctrl = reconcile.New(st, reconcile.NewStoreEffectLog(st), reconcilerFunc(
		func(ctx context.Context, c *reconcile.Context) (reconcile.Result, error) {
			err := c.ApplyEffect(ctx, "ensure-workload", "apply desired state", func() error {
				effectsApplied++
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

func runInstance(ctx context.Context, inst *instance, rounds int) {
	for i := range rounds {
		l, err := inst.lm.Acquire(ctx, shard, inst.id, ttl)
		if err != nil {
			log.Printf("[%s] round=%d acquire: %v (skip reconcile)", inst.id, i, err)
			return
		}
		if err := inst.lm.CheckHeld(ctx, shard, inst.id, l.Fencing); err != nil {
			log.Printf("[%s] round=%d fencing check failed: %v", inst.id, i, err)
			return
		}
		if _, err := inst.ctrl.ReconcileOnce(ctx, key); err != nil {
			log.Printf("[%s] round=%d reconcile: %v", inst.id, i, err)
			return
		}
		obj, _ := inst.st.Get(ctx, key)
		log.Printf("[%s] round=%d reconciled: fencing=%d rv=%d gen=%d",
			inst.id, i, l.Fencing, obj.ResourceVersion, obj.Generation)
	}
}
