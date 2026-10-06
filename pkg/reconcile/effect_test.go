package reconcile

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/effect"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/store"
)

// extSystem 模拟真实外部系统（下单/通知）：
//   - applyCalls 统计进入 Apply 的次数（含被外部幂等去重的）；
//   - charges[id] 统计每个 effectID 被真正执行的次数（幂等键去重后）；
//   - crashAfterActionN>0 时，第 N 次真正执行后返回崩溃错误
//     （外部动作已发出、控制器尚未记录完成的 in-doubt 窗口）。
type extSystem struct {
	mu               sync.Mutex
	charges          map[string]int
	applyCalls       atomic.Int64
	crashAfterAction int64
	realActions      atomic.Int64
	failApplyWith    error
}

func newExtSystem() *extSystem {
	return &extSystem{charges: make(map[string]int)}
}

func (e *extSystem) Apply(_ context.Context, req EffectRequest) error {
	e.applyCalls.Add(1)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.failApplyWith != nil && e.realActions.Load() == 0 {
		return e.failApplyWith
	}
	n := e.realActions.Add(1)
	if _, dup := e.charges[req.EffectID]; dup {
		// 外部系统识别同一幂等键：不重复执行真实业务。
		e.charges[req.EffectID]++
		e.charges[req.EffectID+"__dedup"]++
		return nil
	}
	e.charges[req.EffectID] = 1
	if n == e.crashAfterAction {
		return errors.New("模拟进程崩溃：外部动作已发出，完成标记未写入")
	}
	return nil
}

func (e *extSystem) realChargeCount(id string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	// 去重命中的调用不计入真实执行次数。
	return e.charges[id] - e.charges[id+"__dedup"]
}

func (e *extSystem) totalRealCharges() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	total := 0
	for id, n := range e.charges {
		if len(id) > len("__dedup") && id[len(id)-len("__dedup"):] == "__dedup" {
			continue
		}
		total += n - e.charges[id+"__dedup"]
	}
	return total
}

type effectEnv struct {
	st     *store.MemoryStore
	lm     *lease.Manager
	ledger *effect.MemoryLedger
	ext    *extSystem
	clk    *lease.FakeClock
}

func newEffectEnv(t *testing.T) *effectEnv {
	t.Helper()
	clk := lease.NewFakeClock(time.Now())
	lm := lease.NewManager(time.Minute)
	lm.SetClock(clk)
	return &effectEnv{
		st:     store.NewMemoryStore(),
		lm:     lm,
		ledger: effect.NewMemoryLedger(),
		ext:    newExtSystem(),
		clk:    clk,
	}
}

func (env *effectEnv) controller(t *testing.T, instance string, policy RecoveryPolicy) *Controller {
	t.Helper()
	ctl := NewController(instance, env.st, env.lm, &applyHandler{}, testLogger(t)).
		WithEffect(EffectConfig{
			Ledger:   env.ledger,
			Effects:  env.ext,
			Recovery: policy,
		})
	return ctl
}

// TestEffectExactlyOnceRedeliveryAndRestart 覆盖：
// 重复投递、重启后重放，同一 effectID 的外部副作用真实发生恰好一次，
// 重复的协调在台账处识别为 Succeeded 直接跳过。
func TestEffectExactlyOnceRedeliveryAndRestart(t *testing.T) {
	env := newEffectEnv(t)
	if _, err := env.st.Create(&store.Object{Key: "order", Data: map[string]string{"sku": "A1", "qty": "2"}}); err != nil {
		t.Fatal(err)
	}
	eid := EffectID("order", DefaultFingerprint(mustGet(t, env.st, "order")))
	t.Logf("场景开始 key=order effectID=%s（指纹由目标状态 Data 派生）", eid)

	ctl := env.controller(t, "inst-1", RecoveryIdempotentRetry)
	ctx := context.Background()

	// 第一次协调：真正生效一次
	res, err := ctl.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil {
		t.Fatalf("首次生效: %v", err)
	}
	if res.Outcome != OutcomeApplied || res.Requeue {
		t.Fatalf("首次应 Applied: %+v", res)
	}
	t.Logf("首次协调 判定=%s effectID=%s holder=%s epoch=%d rv=%d->%d",
		res.Outcome, res.EffectID, res.Holder, res.Epoch, res.RVBefore, res.RVAfter)

	// 重复投递 + 同实例重放 5 次
	for i := 0; i < 5; i++ {
		res, err := ctl.ReconcileDetailed(ctx, "shard-0", "order")
		if err != nil || res.Outcome != OutcomeSkippedDuplicate {
			t.Fatalf("第%d次重复应 SkippedDuplicate: res=%+v err=%v", i+2, res, err)
		}
	}
	t.Logf("同实例重复协调 5 次 判定=SkippedDuplicate（依据=台账Succeeded，外部动作零调用）")

	// 模拟进程重启：全新控制器（内存队列/内存状态清空，台账与存储是共享持久层）
	ctlRestart := env.controller(t, "inst-1", RecoveryIdempotentRetry)
	for i := 0; i < 3; i++ {
		res, err := ctlRestart.ReconcileDetailed(ctx, "shard-0", "order")
		if err != nil || res.Outcome != OutcomeSkippedDuplicate {
			t.Fatalf("重启后重放第%d次: res=%+v err=%v", i+1, res, err)
		}
	}
	t.Logf("进程重启后重放 3 次 判定=SkippedDuplicate（依据=持久台账重启后仍在）")

	if n := env.ext.realChargeCount(eid); n != 1 {
		t.Fatalf("外部真实生效次数=%d，期望恰好 1（明细=%v）", n, env.ext.charges)
	}
	final := mustGet(t, env.st, "order")
	if final.ResourceVersion != 2 || final.Status["applied"] != "true" {
		t.Fatalf("最终状态异常 rv=%d status=%v", final.ResourceVersion, final.Status)
	}
	t.Logf("判定=对外副作用恰好一次(真实执行=%d Apply进入=%d)，最终rv=%d",
		env.ext.realChargeCount(eid), env.ext.applyCalls.Load(), final.ResourceVersion)
}

func mustGet(t *testing.T, st *store.MemoryStore, key string) *store.Object {
	t.Helper()
	o, err := st.Get(key)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// TestEffectInDoubtCrashWindowRecovery 覆盖崩溃在
// “外部动作已发出、但还没记录完成”的中间态：
// 记录留在 Pending；重启重放时必须明确按 in-doubt 处理，
// 用同一 effectID 幂等恢复，外部真实执行仍恰好一次。
func TestEffectInDoubtCrashWindowRecovery(t *testing.T) {
	env := newEffectEnv(t)
	if _, err := env.st.Create(&store.Object{Key: "order", Data: map[string]string{"sku": "B2"}}); err != nil {
		t.Fatal(err)
	}
	eid := EffectID("order", "sku=B2;")
	env.ext.crashAfterAction = 1 // 第 1 次真实外部动作之后“进程崩溃”
	t.Logf("场景: effectID=%s 第一次外部动作成功后、Complete 前崩溃", eid)

	ctl1 := env.controller(t, "inst-1", RecoveryIdempotentRetry)
	res, err := ctl1.ReconcileDetailed(context.Background(), "shard-0", "order")
	if err == nil {
		t.Fatalf("崩溃注入应返回错误，得到 res=%+v", res)
	}
	rec, _ := env.ledger.Get(eid)
	if rec == nil || rec.State != effect.StatePending {
		t.Fatalf("崩溃后台账应留 Pending 记录，得到 %+v", rec)
	}
	t.Logf("崩溃后 判定=记录保持Pending holder=%s epoch=%d attempts=%d（外部动作可能已执行，绝不静默重发）",
		rec.Holder, rec.Epoch, rec.Attempts)

	// 模拟进程重启：新控制器实例重放
	ctl2 := env.controller(t, "inst-1", RecoveryIdempotentRetry)
	res2, err := ctl2.ReconcileDetailed(context.Background(), "shard-0", "order")
	if err != nil {
		t.Fatalf("幂等恢复应成功: %v", err)
	}
	if res2.Outcome != OutcomeRecovered {
		t.Fatalf("应判定为 Recovered，得到 %+v", res2)
	}
	t.Logf("重启重放 判定=%s 依据=Pending记录+同一effectID attempts=%d（外部按幂等键去重）", res2.Outcome, res2.Attempts)

	// 再重放：应已 Succeeded，直接跳过
	res3, err := ctl2.ReconcileDetailed(context.Background(), "shard-0", "order")
	if err != nil || res3.Outcome != OutcomeSkippedDuplicate {
		t.Fatalf("恢复后重放应跳过: res=%+v err=%v", res3, err)
	}
	if n := env.ext.realChargeCount(eid); n != 1 {
		t.Fatalf("崩溃窗口恢复后外部真实执行=%d，期望 1（明细=%v）", n, env.ext.charges)
	}
	final := mustGet(t, env.st, "order")
	t.Logf("判定=in-doubt窗口恢复后对外仍恰好一次(真实执行=%d Apply进入=%d) 最终rv=%d status=%v",
		env.ext.realChargeCount(eid), env.ext.applyCalls.Load(), final.ResourceVersion, final.Status)
}

// TestEffectFailClosedPolicy 外部副作用不幂等时采用 FailClosed：
// in-doubt 记录不会被自动重发，控制器报错等待人工对账 Resolve。
func TestEffectFailClosedPolicy(t *testing.T) {
	env := newEffectEnv(t)
	if _, err := env.st.Create(&store.Object{Key: "pay", Data: map[string]string{"amount": "100"}}); err != nil {
		t.Fatal(err)
	}
	eid := EffectID("pay", "amount=100;")
	env.ext.crashAfterAction = 1

	ctl1 := env.controller(t, "inst-1", RecoveryFailClosed)
	if _, err := ctl1.ReconcileDetailed(context.Background(), "shard-0", "pay"); err == nil {
		t.Fatal("期望注入崩溃错误")
	}
	t.Logf("外部动作发出后崩溃，台账=Pending effectID=%s", eid)

	ctl2 := env.controller(t, "inst-1", RecoveryFailClosed)
	res, err := ctl2.ReconcileDetailed(context.Background(), "shard-0", "pay")
	if !errors.Is(err, ErrInDoubtFailClosed) {
		t.Fatalf("期望 ErrInDoubtFailClosed，得到 res=%+v err=%v", res, err)
	}
	if res.Outcome != OutcomeRecovered {
		t.Fatalf("期望 Recovered 判定，得到 %s", res.Outcome)
	}
	if n := env.ext.applyCalls.Load(); n != 1 {
		t.Fatalf("FailClosed 不得重发外部动作，Apply 进入次数=%d", n)
	}
	t.Logf("FailClosed 判定=不自动重发 Apply进入=%d，等待人工确认外部实际状态", env.ext.applyCalls.Load())

	// 人工对账确认外部确实已执行：Resolve 后重放按已生效跳过
	if _, err := env.ledger.Resolve(eid); err != nil {
		t.Fatal(err)
	}
	res2, err := ctl2.ReconcileDetailed(context.Background(), "shard-0", "pay")
	if err != nil || res2.Outcome != OutcomeSkippedDuplicate {
		t.Fatalf("Resolve 后应跳过: res=%+v err=%v", res2, err)
	}
	if env.ext.totalRealCharges() != 1 {
		t.Fatalf("外部真实执行=%d，期望 1", env.ext.totalRealCharges())
	}
	t.Logf("人工Resolve后重放 判定=SkippedDuplicate，全程无静默重复")
}

// TestEffectMultiInstanceConcurrentOnce 多个实例同时抢处理同一对象：
// 租约保证只有一个实例真正生效，其余实例在整个 TTL 窗口内都被判为
// “租约被他人持有”而不生效；外部真实副作用恰好一次。
func TestEffectMultiInstanceConcurrentOnce(t *testing.T) {
	env := newEffectEnv(t)
	if _, err := env.st.Create(&store.Object{Key: "order", Data: map[string]string{"sku": "C3"}}); err != nil {
		t.Fatal(err)
	}
	eid := EffectID("order", "sku=C3;")
	t.Logf("场景: 8 个实例并发协调同一对象 effectID=%s", eid)

	const n = 8
	var wg sync.WaitGroup
	var outcomesMu sync.Mutex
	terminal := map[string]Outcome{}
	var rejected int64
	var startWg sync.WaitGroup
	startWg.Add(1)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := "inst-" + string(rune('1'+idx))
			ctl := env.controller(t, id, RecoveryIdempotentRetry)
			startWg.Wait() // 尽量同时起跑
			res, err := ctl.ReconcileDetailed(context.Background(), "shard-0", "order")
			if err != nil {
				t.Errorf("%s reconcile err: %v", id, err)
				return
			}
			if res.Outcome == OutcomeRequeued && res.Detail == lease.ErrHeldByOther.Error() {
				atomic.AddInt64(&rejected, 1)
				return
			}
			outcomesMu.Lock()
			terminal[id] = res.Outcome
			outcomesMu.Unlock()
		}(i)
	}
	startWg.Done()
	wg.Wait()

	applied := 0
	var appliedBy string
	for id, o := range terminal {
		if o == OutcomeApplied {
			applied++
			appliedBy = id
		}
	}
	t.Logf("并发结果: 生效者=%s(%d) 被租约拒绝=%d 其他终态=%v", appliedBy, applied, atomic.LoadInt64(&rejected), terminal)
	if applied != 1 {
		t.Fatalf("Applied 恰好 1 个实例，实际 %d（%v）", applied, terminal)
	}
	if int(atomic.LoadInt64(&rejected))+applied != n {
		t.Fatalf("其余实例应全部被租约拒绝: rejected=%d", atomic.LoadInt64(&rejected))
	}
	if n2 := env.ext.realChargeCount(eid); n2 != 1 {
		t.Fatalf("外部真实执行=%d，期望 1（明细=%v）", n2, env.ext.charges)
	}
	snap, _ := env.lm.Snapshot("shard-0")
	t.Logf("判定=8实例并发唯一生效者=%s holder=%s epoch=%d 被拒实例=%d 外部真实执行=1",
		appliedBy, snap.Holder, snap.Epoch, atomic.LoadInt64(&rejected))
}

// TestEffectTakeoverFencesLateAction 覆盖换主时的 fencing 规则：
// inst-1 生效时崩溃在 in-doubt 窗口（Pending 记录 epoch=1）；
// 租约过期后 inst-2 以 epoch=2 接管并完成；
// inst-1 迟到的完成/迟到的重放都必须判废，不得盖掉 inst-2 的结果。
func TestEffectTakeoverFencesLateAction(t *testing.T) {
	env := newEffectEnv(t)
	if _, err := env.st.Create(&store.Object{Key: "order", Data: map[string]string{"sku": "D4"}}); err != nil {
		t.Fatal(err)
	}
	eid := EffectID("order", "sku=D4;")
	env.ext.crashAfterAction = 1
	ctx := context.Background()

	// inst-1 第一轮：外部动作发出后崩溃，台账 Pending(holder=inst-1,epoch=1)
	ctl1 := env.controller(t, "inst-1", RecoveryIdempotentRetry)
	if _, err := ctl1.ReconcileDetailed(ctx, "shard-0", "order"); err == nil {
		t.Fatal("期望注入崩溃错误")
	}
	rec, _ := env.ledger.Get(eid)
	t.Logf("inst-1 崩溃: 台账Pending holder=%s epoch=%d，外部动作已发出 1 次", rec.Holder, rec.Epoch)

	// inst-1 尚未失联时 inst-2 试图协调：租约未过期，拒绝生效
	ctl2 := env.controller(t, "inst-2", RecoveryIdempotentRetry)
	res, err := ctl2.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil || res.Outcome != OutcomeRequeued || res.Detail != lease.ErrHeldByOther.Error() {
		t.Fatalf("租约未过期时 inst-2 应被拒: res=%+v err=%v", res, err)
	}
	t.Logf("inst-2 接管前协调 判定=Requeued 依据=%v（不允许双主生效）", lease.ErrHeldByOther)

	// inst-1 失联超过 TTL，inst-2 接管
	env.clk.Advance(2 * time.Minute)
	res2, err := ctl2.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil {
		t.Fatalf("inst-2 接管恢复: %v", err)
	}
	if res2.Outcome != OutcomeRecovered || res2.Epoch != 2 || res2.Holder != "inst-2" {
		t.Fatalf("接管恢复应为 inst-2/epoch=2/Recovered: %+v", res2)
	}
	t.Logf("inst-2 接管 判定=%s holder=%s epoch=%d attempts=%d（同一effectID幂等恢复）",
		res2.Outcome, res2.Holder, res2.Epoch, res2.Attempts)
	rec2, _ := env.ledger.Get(eid)
	if rec2.Holder != "inst-2" || rec2.Epoch != 2 || rec2.State != effect.StateSucceeded {
		t.Fatalf("台账最终归属错误: %+v", rec2)
	}

	// 迟到场景 A：inst-1“复活”，用旧 epoch=1 试图完成同一 effectID -> 台账直接 fencing 拒绝
	if _, cerr := env.ledger.Complete(eid, "inst-1", 1); !errors.Is(cerr, effect.ErrFenceRejected) {
		t.Fatalf("旧持有者迟到完成应被拒，得到 %v", cerr)
	}
	t.Logf("inst-1 旧epoch=1 迟到Complete 判定=%v（记录归属=inst-2/epoch=2）", effect.ErrFenceRejected)

	// 迟到场景 B：inst-1 重新协调（其旧租约已过期）。它 Acquire 时 inst-2 持有未过期租约，
	// 因此 currentFence 阶段就被拒；哪怕它续租成功（TTL 又过期），台账已是 Succeeded，
	// 也只会 SkippedDuplicate，绝不重发外部动作。
	res3, err := ctl1.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil || res3.Outcome != OutcomeRequeued {
		t.Fatalf("旧主人复活协调应被租约拦住: res=%+v err=%v", res3, err)
	}
	t.Logf("inst-1 复活迟到协调 判定=Requeued 依据=%v（新主人 inst-2 租约有效）", lease.ErrHeldByOther)

	// inst-2 视角再重放：Succeeded 跳过
	res4, err := ctl2.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil || res4.Outcome != OutcomeSkippedDuplicate {
		t.Fatalf("新主人重放应跳过: res=%+v err=%v", res4, err)
	}
	if n := env.ext.realChargeCount(eid); n != 1 {
		t.Fatalf("接管全程外部真实执行=%d，期望 1（明细=%v）", n, env.ext.charges)
	}
	final := mustGet(t, env.st, "order")
	t.Logf("判定=换主后旧实例动作全部判废，唯一生效 holder=inst-2 epoch=2，外部真实执行=1 rv=%d",
		final.ResourceVersion)
}

// TestEffectTargetChangeAllowsReapply 覆盖“再生效”规则：
//   - 首次生效后未变更重放 -> 不重复触发；
//   - 无关字段抖动（自定义指纹忽略 noise 键）-> 指纹不变，不重新生效；
//   - 目标状态实质变化（Data 中业务键变化）-> 新 effectID，再真正生效一次；
//   - 新状态生效后，对新指纹的重放也不重复。
func TestEffectTargetChangeAllowsReapply(t *testing.T) {
	env := newEffectEnv(t)
	if _, err := env.st.Create(&store.Object{Key: "order", Data: map[string]string{"sku": "E5", "noise": "j1"}}); err != nil {
		t.Fatal(err)
	}
	// 自定义指纹：只看业务键 sku，忽略 noise（无关抖动）
	fpFunc := func(o *store.Object) string { return "sku=" + o.Data["sku"] + ";" }
	ctx := context.Background()
	ctl := NewController("inst-1", env.st, env.lm, &applyHandler{}, testLogger(t)).
		WithEffect(EffectConfig{Ledger: env.ledger, Effects: env.ext, Recovery: RecoveryIdempotentRetry, Fingerprint: fpFunc})

	idV1 := EffectID("order", "sku=E5;")
	res, err := ctl.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil || res.Outcome != OutcomeApplied || res.EffectID != idV1 {
		t.Fatalf("v1 首次生效: res=%+v err=%v", res, err)
	}
	t.Logf("v1 生效 effectID=%s 指纹=sku=E5;", idV1)

	// 未变更重放：跳过
	if r, _ := ctl.ReconcileDetailed(ctx, "shard-0", "order"); r.Outcome != OutcomeSkippedDuplicate {
		t.Fatalf("未变更重放应跳过: %+v", r)
	}
	t.Logf("未变更重放 判定=SkippedDuplicate（同一 effectID）")

	// 无关字段抖动：noise j1->j2，rv 会变但指纹不变
	cur := mustGet(t, env.st, "order")
	cur.Data["noise"] = "j2"
	if _, err := env.st.Update(cur); err != nil {
		t.Fatal(err)
	}
	r2, err := ctl.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil || r2.Outcome != OutcomeSkippedDuplicate || r2.EffectID != idV1 {
		t.Fatalf("无关抖动不应再生效: res=%+v err=%v", r2, err)
	}
	t.Logf("noise字段抖动(rv前进,指纹不变) 判定=SkippedDuplicate effectID 仍=%s", idV1)

	// 实质变化：sku E5 -> E6，产生新指纹/新 effectID，允许再生效一次
	cur = mustGet(t, env.st, "order")
	cur.Data["sku"] = "E6"
	if _, err := env.st.Update(cur); err != nil {
		t.Fatal(err)
	}
	idV2 := EffectID("order", "sku=E6;")
	r3, err := ctl.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil || r3.Outcome != OutcomeApplied || r3.EffectID != idV2 {
		t.Fatalf("实质变化应再生效一次: res=%+v err=%v", r3, err)
	}
	t.Logf("目标实质变化 指纹=sku=E6; 新effectID=%s 判定=Applied（再生效一次）", idV2)

	// 新状态的重放同样不重复
	r4, _ := ctl.ReconcileDetailed(ctx, "shard-0", "order")
	if r4.Outcome != OutcomeSkippedDuplicate {
		t.Fatalf("v2 重放应跳过: %+v", r4)
	}
	if env.ext.realChargeCount(idV1) != 1 || env.ext.realChargeCount(idV2) != 1 {
		t.Fatalf("每个目标版本各恰好一次: v1=%d v2=%d 明细=%v",
			env.ext.realChargeCount(idV1), env.ext.realChargeCount(idV2), env.ext.charges)
	}
	t.Logf("判定=实质变化再生效、抖动/重放不重复：v1真实执行=1 v2真实执行=1，两个 effectID 各自去重")
}

// TestEffectCrashAfterCompleteBeforeStatus 崩溃在“外部动作完成、台账 Succeeded，
// 但对象 Status 还没回写”的窗口：重放只允许幂等补写 Status，外部动作绝不重发。
func TestEffectCrashAfterCompleteBeforeStatus(t *testing.T) {
	env := newEffectEnv(t)
	_, err := env.st.Create(&store.Object{Key: "order", Data: map[string]string{"sku": "F7"}})
	if err != nil {
		t.Fatal(err)
	}
	eid := EffectID("order", "sku=F7;")
	ctx := context.Background()
	ctl := env.controller(t, "inst-1", RecoveryIdempotentRetry)

	// 先正常生效一次，随后手动把 Status 回退、rv 回退到“未回写”形态：
	// 保留台账 Succeeded 记录，模拟完成标记与状态回写之间的崩溃。
	if _, err := ctl.ReconcileDetailed(ctx, "shard-0", "order"); err != nil {
		t.Fatal(err)
	}
	cur := mustGet(t, env.st, "order")
	cur.Status = map[string]string{}
	if _, err := env.st.Update(cur); err != nil { // 模拟外部也发生过一次写，rv 前进
		t.Fatal(err)
	}
	before := mustGet(t, env.st, "order")
	t.Logf("构造崩溃窗口: 台账=Succeeded(effectID=%s) 对象 status=%v rv=%d", eid, before.Status, before.ResourceVersion)

	res, err := ctl.ReconcileDetailed(ctx, "shard-0", "order")
	if err != nil || res.Outcome != OutcomeSkippedDuplicate {
		t.Fatalf("应仅补写状态并跳过副作用: res=%+v err=%v", res, err)
	}
	after := mustGet(t, env.st, "order")
	if after.Status["applied"] != "true" {
		t.Fatalf("Status 未幂等补齐: %v", after.Status)
	}
	if n := env.ext.realChargeCount(eid); n != 1 {
		t.Fatalf("补写状态不得重发副作用，真实执行=%d", n)
	}
	t.Logf("判定=台账Succeeded下重放仅补写Status(rv=%d)，外部真实执行仍=1（Apply总进入=%d）",
		after.ResourceVersion, env.ext.applyCalls.Load())
}

// TestEffectRunLoopExactlyOnce 通过 Run 队列循环端到端验证：
// 同一 key 多次入队 + 处理过程中再次入队，最终外部副作用恰好一次。
func TestEffectRunLoopExactlyOnce(t *testing.T) {
	env := newEffectEnv(t)
	if _, err := env.st.Create(&store.Object{Key: "order", Data: map[string]string{"sku": "H8"}}); err != nil {
		t.Fatal(err)
	}
	eid := EffectID("order", "sku=H8;")
	ctl := env.controller(t, "inst-1", RecoveryIdempotentRetry)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		ctl.Run(ctx, "shard-0")
		close(done)
	}()

	for i := 0; i < 10; i++ {
		ctl.Queue.Add("order") // 队列按 key 去重
	}
	// 等收敛
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec, _ := env.ledger.Get(eid)
		if rec != nil && rec.State == effect.StateSucceeded && ctl.Queue.Len() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	<-done

	if n := env.ext.realChargeCount(eid); n != 1 {
		t.Fatalf("Run 循环端到端外部真实执行=%d，期望 1（明细=%v）", n, env.ext.charges)
	}
	final := mustGet(t, env.st, "order")
	t.Logf("判定=Run循环+队列去重+台账去重：10次入队外部真实执行=1 rv=%d status=%v",
		final.ResourceVersion, final.Status)
}
