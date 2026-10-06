package reconcile

import (
	"context"
	"errors"
	"sync"

	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/effect"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/store"
)

// ErrInDoubtFailClosed 在 RecoveryFailClosed 策略下遇到 Pending in-doubt
// 记录时返回：控制器不自动重试外部动作，等待人工对账（Ledger.Resolve）。
var ErrInDoubtFailClosed = errors.New("effect in-doubt: fail-closed, manual resolution required")

// keyLock 提供按 key 串行化的本地互斥，避免同实例并发协调同一对象时
// 在 claim/外部动作之间产生竞态（多实例互斥由租约 epoch 保证）。
type keyLock struct {
	mu    sync.Mutex
	locks map[string]*sync.Mutex
}

func newKeyLock() *keyLock { return &keyLock{locks: make(map[string]*sync.Mutex)} }

func (k *keyLock) lock(key string) func() {
	k.mu.Lock()
	m, ok := k.locks[key]
	if !ok {
		m = &sync.Mutex{}
		k.locks[key] = m
	}
	k.mu.Unlock()
	m.Lock()
	return m.Unlock
}

// reconcileEffect 是启用持久化台账后的 exactly-once 协调流程。
//
// 阶段顺序（每一步的判定都带依据入结果/日志）：
//  1. 租约：拿到当前 holder/epoch（fencing token），被他人持有则重排队；
//  2. 读对象，按目标状态（Data）算指纹与 effectID；
//  3. 查台账：
//     - Succeeded：副作用绝不重发，仅幂等补写 Status；
//     - Pending：崩溃 in-doubt 窗口，按归属决定接管恢复或失败关闭；
//     - 无记录：Claim(Pending) 后首次触发；
//  4. 触发外部动作前后各做一次 FenceOK；Complete 再用台账 fencing 兜底；
//  5. 乐观并发回写 Status，冲突只重读，不重发副作用。
func (c *Controller) reconcileEffect(ctx context.Context, shard, key string) (*ReconcileResult, error) {
	unlock := c.keyLocks.lock(key)
	defer unlock()

	cfg := c.effect
	res := &ReconcileResult{Outcome: OutcomeRequeued, Holder: c.Instance}

	// 1. 租约 fencing token。
	epoch, ok := c.currentFence(shard, key, res)
	if !ok {
		return res, nil
	}
	res.Epoch = epoch

	// 2. 读对象 + 派生 effectID。
	obj, err := c.Store.Get(key)
	if err != nil {
		res.Detail = err.Error()
		return res, err
	}
	if obj == nil {
		c.logf("key=%s shard=%s 判定=对象已删除 -> 无需生效", key, shard)
		res.Outcome = OutcomeNoObject
		res.Requeue = false
		return res, nil
	}
	res.RVBefore = obj.ResourceVersion
	fp := cfg.Fingerprint(obj)
	effectID := EffectID(key, fp)
	res.EffectID = effectID
	c.logf("key=%s shard=%s rv=%d holder=%s epoch=%d effectID=%s 指纹=%q",
		key, shard, obj.ResourceVersion, c.Instance, epoch, effectID, fp)

	desired, derr := c.Handler.Reconcile(ctx, obj)
	if derr != nil {
		c.logf("key=%s effectID=%s 判定=期望状态计算失败 依据=%v -> 重试（未触发外部动作）", key, effectID, derr)
		res.Requeue = true
		res.Detail = derr.Error()
		return res, derr
	}

	// 3. 查台账决定走哪条分支。
	rec, gerr := cfg.Ledger.Get(effectID)
	if gerr != nil {
		res.Requeue = true
		res.Detail = gerr.Error()
		return res, gerr
	}
	switch {
	case rec != nil && rec.State == effect.StateSucceeded:
		return c.branchAlreadyDone(ctx, shard, key, obj, desired, rec, res), nil
	case rec != nil && rec.State == effect.StatePending:
		return c.branchInDoubt(ctx, shard, key, obj, desired, rec, epoch, res)
	default:
		return c.branchFresh(ctx, shard, key, obj, desired, fp, effectID, epoch, res)
	}
}

// currentFence 返回当前实例在该 shard 上的有效租约 epoch；拿不到返回 false。
func (c *Controller) currentFence(shard, key string, res *ReconcileResult) (int64, bool) {
	if !c.Leases.HeldBy(shard, c.Instance) {
		rec, aerr := c.Leases.Acquire(shard, c.Instance)
		if aerr != nil {
			snap, _ := c.Leases.Snapshot(shard)
			c.logf("key=%s shard=%s 判定=租约被他人持有 依据=%v 当前holder=%s 当前epoch=%d 本实例=%s -> 重试不生效",
				key, shard, aerr, snap.Holder, snap.Epoch, c.Instance)
			res.Requeue = true
			res.Detail = aerr.Error()
			return 0, false
		}
		c.logf("key=%s shard=%s 判定=获得租约 holder=%s leaseVersion=%d epoch=%d",
			key, shard, rec.Holder, rec.Version, rec.Epoch)
		return rec.Epoch, true
	}
	snap, _ := c.Leases.Snapshot(shard)
	return snap.Epoch, true
}

// branchFresh 无台账记录：先 Claim(Pending) 持久化意图，再首次触发外部动作。
func (c *Controller) branchFresh(ctx context.Context, shard, key string, obj *store.Object,
	desired map[string]string, fp, effectID string, epoch int64, res *ReconcileResult,
) (*ReconcileResult, error) {
	rec, cerr := c.effect.Ledger.Claim(effectID, effect.ClaimKey{Key: key, Fingerprint: fp}, c.Instance, epoch)
	if cerr != nil {
		// 并发竞态：别人刚登记。安全起见本轮重排，下轮按 Pending/Succeeded 判定，不触发新动作。
		c.logf("key=%s effectID=%s 判定=claim竞态 依据=%v -> 重读台账后重试", key, effectID, cerr)
		res.Requeue = true
		res.Detail = cerr.Error()
		return res, nil
	}
	c.logf("key=%s effectID=%s 判定=新生效意图已持久化 state=%s holder=%s epoch=%d",
		key, effectID, rec.State, rec.Holder, rec.Epoch)

	if err := c.invokeExternal(ctx, shard, key, obj, effectID, fp, rec.Attempts, epoch, res); err != nil {
		return res, err
	}
	res.Outcome = OutcomeApplied
	return c.finishWithStatus(shard, key, obj, desired, effectID, epoch, res), nil
}

// branchInDoubt 台账存在 Pending 记录（崩溃窗口：外部动作可能已经发出）。
func (c *Controller) branchInDoubt(ctx context.Context, shard, key string, obj *store.Object,
	desired map[string]string, rec *effect.Record, currentEpoch int64, res *ReconcileResult,
) (*ReconcileResult, error) {
	res.Attempts = rec.Attempts
	// 记录归属与当前 fencing 比较，决定我们是不是“迟到的旧持有者”。
	if rec.Holder != c.Instance || rec.Epoch != currentEpoch {
		// 当前持有者不是记录上的人：若当前租约仍在记录旧主人手里，
		// 本轮根本进不到这里（currentFence 已拦）；能进来说明我们是接管后的新主人。
		if !c.Leases.FenceOK(shard, c.Instance, currentEpoch) {
			c.logf("key=%s effectID=%s 判定=迟到旧持有者 依据=记录holder=%s 记录epoch=%d 当前实例=%s 当前epoch=%d 租约已失效 -> 拒绝生效",
				key, rec.ID, rec.Holder, rec.Epoch, c.Instance, currentEpoch)
			res.Outcome = OutcomeFenced
			res.Requeue = true
			return res, nil
		}
		// 新主人接管 in-doubt 记录。
		rec2, rerr := c.effect.Ledger.Reclaim(rec.ID, c.Instance, currentEpoch)
		if rerr != nil {
			c.logf("key=%s effectID=%s 判定=接管Pending失败 依据=%v -> 重试", key, rec.ID, rerr)
			res.Requeue = true
			res.Detail = rerr.Error()
			return res, nil
		}
		c.logf("key=%s effectID=%s 判定=接管in-doubt记录 旧holder=%s 旧epoch=%d 新holder=%s 新epoch=%d attempt=%d",
			key, rec.ID, rec.Holder, rec.Epoch, rec2.Holder, rec2.Epoch, rec2.Attempts)
		rec = rec2
	}

	switch c.effect.Recovery {
	case RecoveryFailClosed:
		c.logf("key=%s effectID=%s 判定=in-doubt失败关闭 依据=外部动作可能已发出且未确认 策略=FailClosed 等待人工Resolve（不重发）",
			key, rec.ID)
		res.Outcome = OutcomeRecovered
		res.Requeue = false
		res.Attempts = rec.Attempts
		return res, ErrInDoubtFailClosed
	default:
		c.logf("key=%s effectID=%s 判定=in-doubt幂等恢复 依据=同一effectID重试，外部按幂等键去重 attempt=%d",
			key, rec.ID, rec.Attempts)
	}

	if err := c.invokeExternal(ctx, shard, key, obj, rec.ID, rec.Fingerprint, rec.Attempts, currentEpoch, res); err != nil {
		return res, err
	}
	res.Outcome = OutcomeRecovered
	return c.finishWithStatus(shard, key, obj, desired, rec.ID, currentEpoch, res), nil
}

// branchAlreadyDone 台账 Succeeded：同一次生效绝不重发外部动作，
// 仅在 Status 尚未收敛（例如崩溃在记录完成之后、回写之前）时幂等补写。
func (c *Controller) branchAlreadyDone(ctx context.Context, shard, key string, obj *store.Object,
	desired map[string]string, rec *effect.Record, res *ReconcileResult) *ReconcileResult {
	res.Attempts = rec.Attempts
	c.logf("key=%s effectID=%s 判定=已生效跳过副作用 依据=台账Succeeded by holder=%s epoch=%d（重复投递/重放/多实例重复均不再触发）",
		key, rec.ID, rec.Holder, rec.Epoch)
	res.Outcome = OutcomeSkippedDuplicate
	res.Requeue = false
	if desired != nil && !equalMap(desired, obj.Status) {
		c.logf("key=%s effectID=%s 判定=状态未收敛，幂等补写Status（不触发外部动作）", key, rec.ID)
		return c.finishWithStatus(shard, key, obj, desired, rec.ID, rec.Epoch, res)
	}
	return res
}

// invokeExternal 执行对外副作用，前后双 fencing：
// 调用前确认租约有效；调用返回后再次确认，且台账 Complete 必须匹配归属。
func (c *Controller) invokeExternal(ctx context.Context, shard, key string, obj *store.Object,
	effectID, fp string, attempt int, epoch int64, res *ReconcileResult) error {
	if !c.Leases.FenceOK(shard, c.Instance, epoch) {
		c.logf("key=%s effectID=%s 判定=动作前fencing失败 holder=%s epoch=%d -> 不发出外部动作，重试",
			key, effectID, c.Instance, epoch)
		res.Outcome = OutcomeFenced
		res.Requeue = true
		return lease.ErrHeldByOther
	}
	c.logf("key=%s effectID=%s 判定=触发外部副作用 holder=%s epoch=%d attempt=%d（以effectID为外部幂等键）",
		key, effectID, c.Instance, epoch, attempt)
	aerr := c.effect.Effects.Apply(ctx, EffectRequest{
		Object:      obj.DeepCopy(),
		EffectID:    effectID,
		Fingerprint: fp,
		Holder:      c.Instance,
		Epoch:       epoch,
		Attempt:     attempt,
	})
	if aerr != nil {
		// 外部明确失败：记录保持 Pending，重试仍用同一 effectID，不会产生重复。
		c.logf("key=%s effectID=%s 判定=外部动作失败 依据=%v 记录保持Pending -> 同ID重试", key, effectID, aerr)
		res.Requeue = true
		res.Detail = aerr.Error()
		return aerr
	}
	// 动作已发出：先确认这一任租约仍算数，再落完成标记。
	if !c.Leases.FenceOK(shard, c.Instance, epoch) {
		c.logf("key=%s effectID=%s 判定=动作后fencing失败 holder=%s epoch=%d -> 不写完成标记，交由新持有者按同ID恢复",
			key, effectID, c.Instance, epoch)
		res.Outcome = OutcomeFenced
		res.Requeue = true
		return lease.ErrHeldByOther
	}
	if _, cerr := c.effect.Ledger.Complete(effectID, c.Instance, epoch); cerr != nil {
		if errors.Is(cerr, effect.ErrFenceRejected) {
			c.logf("key=%s effectID=%s 判定=完成被fencing拒绝 依据=%v -> 旧持有者迟到完成无效", key, effectID, cerr)
			res.Outcome = OutcomeFenced
			res.Requeue = true
			return cerr
		}
		res.Requeue = true
		res.Detail = cerr.Error()
		return cerr
	}
	c.logf("key=%s effectID=%s 判定=台账记录完成 state=Succeeded holder=%s epoch=%d", key, effectID, c.Instance, epoch)
	res.Attempts = attempt
	return nil
}

// finishWithStatus 台账完成后乐观并发回写 Status；冲突只重读重试，绝不重发副作用。
func (c *Controller) finishWithStatus(shard, key string, obj *store.Object, desired map[string]string,
	effectID string, epoch int64, res *ReconcileResult) *ReconcileResult {
	res.Requeue = false
	if desired == nil {
		return res
	}
	if equalMap(desired, obj.Status) {
		c.logf("key=%s effectID=%s 判定=Status已一致，跳过回写", key, effectID)
		return res
	}
	obj.Status = desired
	updated, uerr := c.Store.Update(obj)
	if uerr != nil {
		if reason, ok := store.IsConflict(uerr); ok {
			c.logf("key=%s effectID=%s 判定=状态回写冲突 原因=%s -> 仅重读重试，副作用不重发", key, effectID, reason)
			res.Requeue = true
			res.Outcome = OutcomeRequeued
			res.Detail = string(reason)
			return res
		}
		res.Requeue = true
		res.Detail = uerr.Error()
		return res
	}
	res.RVAfter = updated.ResourceVersion
	c.logf("key=%s effectID=%s 判定=状态回写成功 rv=%d->%d", key, effectID, obj.ResourceVersion, updated.ResourceVersion)
	return res
}
