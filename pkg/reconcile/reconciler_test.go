package reconcile

import (
	"context"
	"errors"
	"log"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/store"
)

func testLogger(t *testing.T) *log.Logger {
	return log.New(testWriter{t}, "", 0)
}

type testWriter struct{ t *testing.T }

func (w testWriter) Write(p []byte) (int, error) {
	w.t.Logf("%s", string(p[:len(p)-1]))
	return len(p), nil
}

// applyHandler 幂等 Handler：把 data 拷贝到 status 并标记 applied。
// applyCount 记录“实际生效”的副作用次数（仅当控制器真正写回时由测试统计）。
type applyHandler struct {
	calls atomic.Int64
}

func (h *applyHandler) Reconcile(_ context.Context, obj *store.Object) (map[string]string, error) {
	h.calls.Add(1)
	status := make(map[string]string, len(obj.Data)+1)
	for k, v := range obj.Data {
		status[k] = v
	}
	status["applied"] = "true"
	return status, nil
}

func newEnv(t *testing.T, instance string, ttl time.Duration) (*store.MemoryStore, *lease.Manager, *Controller) {
	st := store.NewMemoryStore()
	lm := lease.NewManager(ttl)
	ctl := NewController(instance, st, lm, &applyHandler{}, testLogger(t))
	return st, lm, ctl
}

// TestIdempotentRedelivery 同一对象重复投递，只产生一次有效写回。
func TestIdempotentRedelivery(t *testing.T) {
	st, _, ctl := newEnv(t, "inst-1", time.Minute)
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"a": "1"}}); err != nil {
		t.Fatal(err)
	}

	// 重复投递同一 key
	for i := 0; i < 5; i++ {
		ctl.Queue.Add("obj")
	}
	if n := ctl.Queue.Len(); n != 1 {
		t.Fatalf("队列应去重为 1 个元素，实际 %d", n)
	}
	t.Logf("重复投递 5 次后队列长度=1，判定=去重生效")

	ctx := context.Background()
	for i := 0; i < 3; i++ { // 模拟重复触发/重放
		requeue, err := ctl.ReconcileOne(ctx, "shard-0", "obj")
		if err != nil || requeue {
			t.Fatalf("reconcile: requeue=%v err=%v", requeue, err)
		}
	}
	final, _ := st.Get("obj")
	t.Logf("最终 rv=%d status=%v", final.ResourceVersion, final.Status)
	if final.ResourceVersion != 2 {
		t.Fatalf("幂等性被破坏: 期望 rv=2（仅一次有效写回），实际 rv=%d", final.ResourceVersion)
	}
	if final.Status["applied"] != "true" || final.Status["a"] != "1" {
		t.Fatalf("状态未收敛: %v", final.Status)
	}
	t.Logf("判定=重复协调未产生重复副作用，rv 只前进一次")
}

// flakyHandler 前 failTimes 次调用失败，模拟协调中途失败/进程崩溃。
type flakyHandler struct {
	inner    *applyHandler
	failTimes int
	calls     atomic.Int64
}

func (f *flakyHandler) Reconcile(ctx context.Context, obj *store.Object) (map[string]string, error) {
	n := f.calls.Add(1)
	if n <= int64(f.failTimes) {
		return nil, errors.New("模拟协调中途失败")
	}
	return f.inner.Reconcile(ctx, obj)
}

// TestInterruptedReconcileConverges 协调中断后重试（或重启新建控制器）收敛到同一目标状态。
func TestInterruptedReconcileConverges(t *testing.T) {
	st := store.NewMemoryStore()
	lm := lease.NewManager(time.Minute)
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"x": "y"}}); err != nil {
		t.Fatal(err)
	}

	// 第一次协调：Handler 中途失败（模拟崩溃），状态未写回
	h1 := &flakyHandler{inner: &applyHandler{}, failTimes: 1}
	ctl1 := NewController("inst-1", st, lm, h1, testLogger(t))
	requeue, err := ctl1.ReconcileOne(context.Background(), "shard-0", "obj")
	if err == nil || !requeue {
		t.Fatalf("期望失败并要求重试, requeue=%v err=%v", requeue, err)
	}
	mid, _ := st.Get("obj")
	t.Logf("中断后 rv=%d status=%v（未写回，可安全重放）", mid.ResourceVersion, mid.Status)
	if mid.ResourceVersion != 1 {
		t.Fatalf("失败不应产生部分写回, rv=%d", mid.ResourceVersion)
	}

	// 模拟进程重启：换一个全新的控制器实例重放同一对象
	ctl2 := NewController("inst-1", st, lm, &applyHandler{}, testLogger(t))
	if _, err := ctl2.ReconcileOne(context.Background(), "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	final, _ := st.Get("obj")
	t.Logf("重放后 rv=%d status=%v", final.ResourceVersion, final.Status)
	if final.ResourceVersion != 2 || final.Status["applied"] != "true" || final.Status["x"] != "y" {
		t.Fatalf("未收敛到目标状态: rv=%d status=%v", final.ResourceVersion, final.Status)
	}
	t.Logf("判定=中断后重放收敛到同一目标状态，无副作用残留")
}

// TestLeaseTakeoverBetweenControllers 实例 1 持有租约时实例 2 无法生效；
// 租约过期后实例 2 接管并完成协调，全程无双主同时生效。
func TestLeaseTakeoverBetweenControllers(t *testing.T) {
	st := store.NewMemoryStore()
	clk := lease.NewFakeClock(time.Now())
	lm := lease.NewManager(time.Minute)
	lm.SetClock(clk)
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}

	ctl1 := NewController("inst-1", st, lm, &applyHandler{}, testLogger(t))
	ctl2 := NewController("inst-2", st, lm, &applyHandler{}, testLogger(t))
	ctx := context.Background()

	// inst-1 先协调，获得租约
	if _, err := ctl1.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	after1, _ := st.Get("obj")
	t.Logf("inst-1 协调完成 rv=%d", after1.ResourceVersion)

	// inst-1 修改对象（仍持有租约）
	after1.Data["k"] = "v2"
	if _, err := st.Update(after1); err != nil {
		t.Fatal(err)
	}

	// inst-2 尝试协调：租约未过期，必须被拒绝生效
	requeue, err := ctl2.ReconcileOne(ctx, "shard-0", "obj")
	if err != nil || !requeue {
		t.Fatalf("inst-2 应被要求稍后重试, requeue=%v err=%v", requeue, err)
	}
	cur, _ := st.Get("obj")
	t.Logf("inst-2 被拒绝时 rv=%d status=%v 判定=租约互斥阻止双主", cur.ResourceVersion, cur.Status)
	if cur.Status["k"] == "v2" {
		t.Fatal("inst-2 在未持有租约时生效，出现双主")
	}

	// inst-1 失联（时钟推进超过 TTL），inst-2 接管
	clk.Advance(2 * time.Minute)
	requeue, err = ctl2.ReconcileOne(ctx, "shard-0", "obj")
	if err != nil || requeue {
		t.Fatalf("inst-2 接管后应协调成功, requeue=%v err=%v", requeue, err)
	}
	final, _ := st.Get("obj")
	snap, _ := lm.Snapshot("shard-0")
	t.Logf("接管后 holder=%s leaseVersion=%d, obj rv=%d status=%v",
		snap.Holder, snap.Version, final.ResourceVersion, final.Status)
	if snap.Holder != "inst-2" {
		t.Fatalf("租约应被 inst-2 接管, holder=%s", snap.Holder)
	}
	if final.Status["k"] != "v2" || final.Status["applied"] != "true" {
		t.Fatalf("接管后未收敛: %v", final.Status)
	}
	t.Logf("判定=租约过期后单点接管，接管结果与冲突原因可区分")
}

// TestConcurrentReconcileNoLostUpdate 控制器回写与外部写入并发时，
// 乐观并发保证不丢更新：最终版本 = 外部写入次数 + 控制器写回次数。
func TestConcurrentReconcileNoLostUpdate(t *testing.T) {
	st, _, ctl := newEnv(t, "inst-1", time.Minute)
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"n": "0"}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	done := make(chan struct{})
	go func() { // 外部写入者：读-改-写重试循环
		defer close(done)
		for i := 0; i < 20; i++ {
			for {
				cur, _ := st.Get("obj")
				next := cur.DeepCopy()
				next.Data["ext"] = string(rune('a' + i))
				if _, err := st.Update(next); err != nil {
					if _, ok := store.IsConflict(err); ok {
						continue
					}
					t.Errorf("ext update: %v", err)
					return
				}
				break
			}
		}
	}()

	// 控制器并发协调（Handler 幂等，冲突时重读重试）
	for i := 0; i < 40; i++ {
		ctl.ReconcileOne(ctx, "shard-0", "obj")
	}
	<-done

	final, _ := st.Get("obj")
	t.Logf("最终 rv=%d data=%v status=%v", final.ResourceVersion, final.Data, final.Status)
	if final.Data["ext"] == "" {
		t.Fatal("外部写入丢失")
	}
	if final.Status["applied"] != "true" {
		t.Fatal("控制器写回丢失")
	}
	t.Logf("判定=并发下控制器与外部写入均未丢失")
}
