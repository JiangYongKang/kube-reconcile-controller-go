package reconcile

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/effect"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/store"
)

// idemSink 模拟支持幂等键的外部系统：同一 Effect.ID 只真正生效一次。
type idemSink struct {
	mu       sync.Mutex
	calls    map[string]int // id -> Trigger 调用次数
	effected map[string]map[string]string
	failNext atomic.Int64 // 前 N 次 Trigger 在生效后返回错误，模拟崩溃窗口
}

func newIdemSink() *idemSink {
	return &idemSink{calls: make(map[string]int), effected: make(map[string]map[string]string)}
}

func (s *idemSink) Trigger(_ context.Context, e effect.Effect) error {
	s.mu.Lock()
	s.calls[e.ID]++
	if _, ok := s.effected[e.ID]; !ok {
		cp := make(map[string]string, len(e.Payload))
		for k, v := range e.Payload {
			cp[k] = v
		}
		s.effected[e.ID] = cp
	}
	s.mu.Unlock()
	if s.failNext.Load() > 0 {
		s.failNext.Add(-1)
		return fmt.Errorf("模拟崩溃：动作已发出，完成未记录")
	}
	return nil
}

// realEffects 返回外部系统真正生效的次数（按幂等键去重后）。
func (s *idemSink) realEffects() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.effected)
}

// jitterHandler 每次在期望状态里加入变化的抖动字段 "_ts"，
// 使状态写回每次都发生，从而迫使协调走到生效判定路径。
type jitterHandler struct {
	n atomic.Int64
}

func (h *jitterHandler) Reconcile(_ context.Context, obj *store.Object) (map[string]string, error) {
	status := make(map[string]string, len(obj.Data)+2)
	for k, v := range obj.Data {
		status[k] = v
	}
	status["applied"] = "true"
	status["_ts"] = fmt.Sprintf("call-%d", h.n.Add(1)) // 抖动字段，不计入指纹
	return status, nil
}

func newEffectEnv(t *testing.T, instance string, lm *lease.Manager, st *store.MemoryStore, j *effect.Journal, sink *idemSink, h Handler) *Controller {
	ctl := NewController(instance, st, lm, h, testLogger(t))
	ctl.Journal = j
	ctl.Sink = sink
	return ctl
}

// TestEffectExactlyOnceRedelivery 重复投递/重复触发同一次协调，
// 对外副作用恰好生效一次，后续判定为「目标状态未变更」。
func TestEffectExactlyOnceRedelivery(t *testing.T) {
	st := store.NewMemoryStore()
	lm := lease.NewManager(time.Minute)
	j := effect.NewJournal()
	sink := newIdemSink()
	ctl := newEffectEnv(t, "inst-1", lm, st, j, sink, &jitterHandler{})
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"replicas": "3"}}); err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	for i := 0; i < 5; i++ { // 模拟重复投递与重放
		requeue, err := ctl.ReconcileOne(ctx, "shard-0", "obj")
		if err != nil || requeue {
			t.Fatalf("第 %d 次协调: requeue=%v err=%v", i+1, requeue, err)
		}
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("重复投递下外部应恰好生效 1 次, 实际 %d", n)
	}
	t.Logf("判定=重复投递 5 次，外部恰好生效 1 次，其余按指纹去重跳过")
}

// TestEffectCrashWindowResume 崩溃卡在「外部动作已发出、完成未记录」：
// 重启续跑时按同一幂等键重放，由外部系统去重，不静默重复。
func TestEffectCrashWindowResume(t *testing.T) {
	st := store.NewMemoryStore()
	lm := lease.NewManager(time.Minute)
	j := effect.NewJournal()
	sink := newIdemSink()
	sink.failNext.Store(1) // 第一次 Trigger 生效后报错：记录停留 InFlight
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"order": "A1"}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// 第一次协调：外部动作已发出，但完成未记录（模拟崩溃）。
	ctl1 := newEffectEnv(t, "inst-1", lm, st, j, sink, &jitterHandler{})
	requeue, err := ctl1.ReconcileOne(ctx, "shard-0", "obj")
	t.Logf("崩溃现场: requeue=%v err=%v", requeue, err)
	if err == nil || !requeue {
		t.Fatalf("崩溃窗口应表现为失败重试, requeue=%v err=%v", requeue, err)
	}

	// 进程重启：全新控制器重放同一对象（共享持久化的 Journal）。
	ctl2 := newEffectEnv(t, "inst-1", lm, st, j, sink, &jitterHandler{})
	requeue, err = ctl2.ReconcileOne(ctx, "shard-0", "obj")
	if err != nil || requeue {
		t.Fatalf("重启续跑: requeue=%v err=%v", requeue, err)
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("崩溃窗口恢复后外部应恰好生效 1 次, 实际 %d", n)
	}
	final, _ := st.Get("obj")
	if final.Status["order"] != "A1" {
		t.Fatalf("恢复后状态未收敛: %v", final.Status)
	}
	t.Logf("判定=崩溃窗口按同一幂等键恢复，外部去重后恰好生效 1 次")
}

// TestEffectRestartReplayNoDuplicate 进程重启后重放已生效过的对象，
// 持久化 Journal 识别目标状态未变更，不重复触发。
func TestEffectRestartReplayNoDuplicate(t *testing.T) {
	st := store.NewMemoryStore()
	lm := lease.NewManager(time.Minute)
	j := effect.NewJournal()
	sink := newIdemSink()
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	ctl1 := newEffectEnv(t, "inst-1", lm, st, j, sink, &jitterHandler{})
	if _, err := ctl1.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	t.Logf("首次生效完成，realEffects=%d", sink.realEffects())

	// 模拟进程重启：全新控制器实例，Journal 持久化保留。
	ctl2 := newEffectEnv(t, "inst-1", lm, st, j, sink, &jitterHandler{})
	for i := 0; i < 3; i++ {
		if _, err := ctl2.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
			t.Fatal(err)
		}
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("重启重放不应重复生效, 实际 %d 次", n)
	}
	t.Logf("判定=重启后重放被持久化去重判据拦截，未重复生效")
}

// TestEffectConcurrentInstances 多实例并发抢同一对象，对外恰好生效一次。
func TestEffectConcurrentInstances(t *testing.T) {
	st := store.NewMemoryStore()
	lm := lease.NewManager(time.Minute)
	j := effect.NewJournal()
	sink := newIdemSink()
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"k": "v"}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 1; i <= 4; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctl := newEffectEnv(t, fmt.Sprintf("inst-%d", i), lm, st, j, sink, &jitterHandler{})
			for n := 0; n < 10; n++ {
				ctl.ReconcileOne(ctx, "shard-0", "obj")
			}
		}(i)
	}
	wg.Wait()
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("4 实例并发抢占，外部应恰好生效 1 次, 实际 %d", n)
	}
	snap, _ := lm.Snapshot("shard-0")
	t.Logf("最终持有者=%s leaseVersion=%d 判定=多实例并发下恰好生效一次", snap.Holder, snap.Version)
}

// gatedHandler 在首次调用时阻塞，模拟实例在「已读对象、未生效」处卡顿。
type gatedHandler struct {
	entered chan struct{}
	gate    chan struct{}
	inner   *jitterHandler
	once    sync.Once
}

func (h *gatedHandler) Reconcile(ctx context.Context, obj *store.Object) (map[string]string, error) {
	h.once.Do(func() {
		close(h.entered)
		<-h.gate
	})
	return h.inner.Reconcile(ctx, obj)
}

// TestStaleInstanceEffectRejected 旧实例在生效前卡顿，分片被接管且
// 目标状态已前进；旧实例迟到的生效动作被围栏判为过期拒绝。
func TestStaleInstanceEffectRejected(t *testing.T) {
	st := store.NewMemoryStore()
	clk := lease.NewFakeClock(time.Now())
	lm := lease.NewManager(time.Minute)
	lm.SetClock(clk)
	j := effect.NewJournal()
	sink := newIdemSink()
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"v": "0"}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	// inst-1 开始协调：拿到租约、读完对象后卡在 Handler 里。
	gated := &gatedHandler{entered: make(chan struct{}), gate: make(chan struct{}), inner: &jitterHandler{}}
	ctl1 := newEffectEnv(t, "inst-1", lm, st, j, sink, gated)
	staleDone := make(chan struct{})
	go func() {
		defer close(staleDone)
		requeue, err := ctl1.ReconcileOne(ctx, "shard-0", "obj")
		t.Logf("inst-1 迟到协调返回: requeue=%v err=%v", requeue, err)
		if err != nil || requeue {
			t.Errorf("旧实例迟到动作应被判定过期后丢弃, requeue=%v err=%v", requeue, err)
		}
	}()
	<-gated.entered
	t.Logf("inst-1 已读对象并卡顿（租约 v1 持有中）")

	// inst-1 失联，inst-2 接管并对 v0 目标状态生效。
	clk.Advance(2 * time.Minute)
	ctl2 := newEffectEnv(t, "inst-2", lm, st, j, sink, &jitterHandler{})
	if _, err := ctl2.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	// 目标状态前进：v0 -> v1，inst-2 再生效一次。
	cur, _ := st.Get("obj")
	cur.Data["v"] = "1"
	if _, err := st.Update(cur); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl2.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	t.Logf("inst-2 接管后已对新目标状态生效，realEffects=%d", sink.realEffects())

	// 放行 inst-1：它带着旧令牌与旧目标状态的迟到生效必须被拒。
	close(gated.gate)
	<-staleDone

	if n := sink.realEffects(); n != 2 {
		t.Fatalf("旧实例迟到动作应被拒绝，外部仍只生效 2 次, 实际 %d", n)
	}
	snap, _ := lm.Snapshot("shard-0")
	final, _ := st.Get("obj")
	t.Logf("holder=%s leaseVersion=%d obj rv=%d status=%v 判定=旧实例迟到生效被围栏拒绝，未覆盖新实例结果",
		snap.Holder, snap.Version, final.ResourceVersion, final.Status)
	if final.Status["v"] != "1" {
		t.Fatalf("新实例结果被覆盖: %v", final.Status)
	}
}

// TestEffectDesiredChangeReexecutes 目标状态实质变更允许再次生效；
// 未变更的重放不重复生效，两种路径在日志中可区分。
func TestEffectDesiredChangeReexecutes(t *testing.T) {
	st := store.NewMemoryStore()
	lm := lease.NewManager(time.Minute)
	j := effect.NewJournal()
	sink := newIdemSink()
	ctl := newEffectEnv(t, "inst-1", lm, st, j, sink, &jitterHandler{})
	if _, err := st.Create(&store.Object{Key: "obj", Data: map[string]string{"replicas": "3"}}); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	if _, err := ctl.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	// 未变更重放：抖动字段变化但实质目标状态不变。
	if _, err := ctl.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("未变更重放不应重复生效, 实际 %d", n)
	}
	t.Logf("未变更重放后 realEffects=%d（抖动字段未触发再生效）", sink.realEffects())

	// 实质变更：replicas 3 -> 5，允许再次生效。
	cur, _ := st.Get("obj")
	cur.Data["replicas"] = "5"
	if _, err := st.Update(cur); err != nil {
		t.Fatal(err)
	}
	if _, err := ctl.ReconcileOne(ctx, "shard-0", "obj"); err != nil {
		t.Fatal(err)
	}
	if n := sink.realEffects(); n != 2 {
		t.Fatalf("目标状态实质变更后应再次生效, 实际 %d", n)
	}
	t.Logf("判定=实质变更再次生效（共 2 次）、未变更重放不重复，两种情况可区分")
}
