package reconcile

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/highcumontoa/kube-reconcile-controller-go/internal/store"
)

// deployReconciler 是测试用业务逻辑：确保一次 "ensure-deployment" 副作用，
// 并把 Status 推进到 Ready。failOnce 可注入一次中断。
type deployReconciler struct {
	mu        sync.Mutex
	applied   map[string]int // action@generation -> 生效次数
	failOnce  bool           // 第一次执行 fn 时注入失败（模拟协调中断）
	failCount int
}

func newDeployReconciler() *deployReconciler {
	return &deployReconciler{applied: map[string]int{}}
}

func (d *deployReconciler) appliedCount(action string, gen int64) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.applied[fmt.Sprintf("%s@%d", action, gen)]
}

func (d *deployReconciler) Reconcile(ctx context.Context, c *Context) (Result, error) {
	err := c.ApplyEffect(ctx, "ensure-deployment", "apply desired spec", func() error {
		d.mu.Lock()
		if d.failOnce {
			d.failOnce = false
			d.failCount++
			d.mu.Unlock()
			return errors.New("injected interruption") // 副作用尚未生效即中断
		}
		d.applied[fmt.Sprintf("ensure-deployment@%d", c.Object.Generation)]++
		d.mu.Unlock()
		return nil
	})
	if err != nil {
		return Result{}, err
	}
	c.SetStatus(ctx, "phase", "Ready")
	c.SetStatus(ctx, "observedGeneration", c.Object.Generation)
	return Result{}, nil
}

func setup(t *testing.T) (context.Context, *store.MemoryStore, *deployReconciler, *Controller) {
	t.Helper()
	st := store.NewMemoryStore()
	rec := newDeployReconciler()
	ctrl := New(st, NewStoreEffectLog(st), rec)
	ctx := context.Background()
	if _, err := st.Create(ctx, &store.Object{
		Key:  "app/web",
		Spec: map[string]any{"replicas": 3},
	}); err != nil {
		t.Fatal(err)
	}
	return ctx, st, rec, ctrl
}

// 重复投递：同一对象协调多次，副作用只生效一次，状态收敛一致。
func TestDuplicateDeliveryIdempotent(t *testing.T) {
	ctx, st, rec, ctrl := setup(t)

	for i := range 5 {
		if _, err := ctrl.ReconcileOnce(ctx, "app/web"); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}

	obj, _ := st.Get(ctx, "app/web")
	t.Logf("deliveries=5 applied=%d rv=%d gen=%d phase=%v (判定: 同(Key,Generation,Action)的副作用只Complete一次)",
		rec.appliedCount("ensure-deployment", 1), obj.ResourceVersion, obj.Generation, obj.Status["phase"])
	if got := rec.appliedCount("ensure-deployment", 1); got != 1 {
		t.Fatalf("effect applied %d times, want 1", got)
	}
	if obj.Status["phase"] != "Ready" {
		t.Fatalf("status not converged: %v", obj.Status)
	}
}

// 协调中断：第一次副作用执行被注入失败，重投后续跑并收敛，副作用不重复。
func TestInterruptedReconcileResumes(t *testing.T) {
	ctx, st, rec, ctrl := setup(t)
	rec.failOnce = true

	if _, err := ctrl.ReconcileOnce(ctx, "app/web"); err == nil {
		t.Fatal("expected first reconcile to fail")
	}
	obj, _ := st.Get(ctx, "app/web")
	t.Logf("after interruption: rv=%d phase=%v applied=%d (判定: 中断时副作用未生效, Status未推进)",
		obj.ResourceVersion, obj.Status["phase"], rec.appliedCount("ensure-deployment", 1))
	if obj.Status["phase"] != nil {
		t.Fatalf("status should not be Ready after interruption, got %v", obj.Status)
	}

	// 重投（可重入）：续跑中断的副作用
	if _, err := ctrl.ReconcileOnce(ctx, "app/web"); err != nil {
		t.Fatalf("retry: %v", err)
	}
	obj, _ = st.Get(ctx, "app/web")
	t.Logf("after retry: rv=%d phase=%v applied=%d (判定: claimed记录续跑, 副作用恰好生效一次)",
		obj.ResourceVersion, obj.Status["phase"], rec.appliedCount("ensure-deployment", 1))
	if got := rec.appliedCount("ensure-deployment", 1); got != 1 {
		t.Fatalf("effect applied %d times, want 1", got)
	}
	if obj.Status["phase"] != "Ready" {
		t.Fatalf("status not converged: %v", obj.Status)
	}
}

// 进程重启：在 claimed 与 done 之间“崩溃”（新控制器实例接管同一存储），
// 重启后重放协调，副作用不重复生效，状态收敛到同一目标。
func TestRestartConverges(t *testing.T) {
	ctx, st, rec, ctrl := setup(t)

	// 模拟崩溃点：副作用记录被认领（claimed）但进程在生效前退出
	eff := Effect{Key: "app/web", Generation: 1, Action: "ensure-deployment"}
	if _, err := ctrl.effects.Claim(ctx, eff); err != nil {
		t.Fatal(err)
	}

	// 新进程：全新的控制器与业务实例，共享同一对象存储
	rec2 := newDeployReconciler()
	ctrl2 := New(st, NewStoreEffectLog(st), rec2)
	for range 3 {
		if _, err := ctrl2.ReconcileOnce(ctx, "app/web"); err != nil {
			t.Fatalf("reconcile after restart: %v", err)
		}
	}

	obj, _ := st.Get(ctx, "app/web")
	t.Logf("after restart: applied(old)=%d applied(new)=%d rv=%d phase=%v (判定: 崩溃遗留的claimed由新实例续跑, 全局只生效一次)",
		rec.appliedCount("ensure-deployment", 1), rec2.appliedCount("ensure-deployment", 1),
		obj.ResourceVersion, obj.Status["phase"])
	total := rec.appliedCount("ensure-deployment", 1) + rec2.appliedCount("ensure-deployment", 1)
	if total != 1 {
		t.Fatalf("effect applied %d times across restart, want 1", total)
	}
	if obj.Status["phase"] != "Ready" {
		t.Fatalf("status not converged after restart: %v", obj.Status)
	}
}

// 并发协调：N 个协程同时协调同一对象，副作用只生效一次，无丢失更新。
func TestConcurrentReconcileSameObject(t *testing.T) {
	ctx, st, rec, ctrl := setup(t)

	const workers = 16
	var wg sync.WaitGroup
	errs := make([]error, workers)
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = ctrl.ReconcileOnce(ctx, "app/web")
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}

	obj, _ := st.Get(ctx, "app/web")
	t.Logf("workers=%d applied=%d rv=%d phase=%v (判定: 分段锁串行化+副作用去重, 并发结果与串行一致)",
		workers, rec.appliedCount("ensure-deployment", 1), obj.ResourceVersion, obj.Status["phase"])
	if got := rec.appliedCount("ensure-deployment", 1); got != 1 {
		t.Fatalf("effect applied %d times, want 1", got)
	}
	if obj.Status["phase"] != "Ready" || obj.Status["observedGeneration"] != int64(1) {
		t.Fatalf("status not converged: %v", obj.Status)
	}
}

// Spec 变更推进 Generation：新一代的副作用独立生效一次，旧代不重复。
func TestGenerationAdvanceReconcilesNewTarget(t *testing.T) {
	ctx, st, rec, ctrl := setup(t)

	if _, err := ctrl.ReconcileOnce(ctx, "app/web"); err != nil {
		t.Fatal(err)
	}
	// 变更 Spec → Generation 2
	obj, _ := st.Get(ctx, "app/web")
	obj.Spec["replicas"] = 5
	if _, err := st.Update(ctx, obj, obj.ResourceVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := ctrl.ReconcileOnce(ctx, "app/web"); err != nil {
		t.Fatal(err)
	}

	obj, _ = st.Get(ctx, "app/web")
	t.Logf("gen=%d applied(gen1)=%d applied(gen2)=%d (判定: 副作用按(Key,Generation,Action)去重, 每代恰好一次)",
		obj.Generation, rec.appliedCount("ensure-deployment", 1), rec.appliedCount("ensure-deployment", 2))
	if rec.appliedCount("ensure-deployment", 1) != 1 || rec.appliedCount("ensure-deployment", 2) != 1 {
		t.Fatalf("unexpected applied counts: %v", rec.applied)
	}
	if obj.Status["observedGeneration"] != int64(2) {
		t.Fatalf("status not tracking gen2: %v", obj.Status)
	}
}
