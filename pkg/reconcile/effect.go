package reconcile

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"

	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/effect"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/store"
)

// Outcome 标识单次协调对外副作用的最终判定，用于结果断言与日志区分。
type Outcome string

const (
	// OutcomeApplied 本次真正触发了外部副作用并确认完成。
	OutcomeApplied Outcome = "Applied"
	// OutcomeSkippedDuplicate 该 effectID 已生效过（重复投递/重放/多实例重复），直接跳过。
	OutcomeSkippedDuplicate Outcome = "SkippedDuplicate"
	// OutcomeRecovered 存在 Pending in-doubt 记录（崩溃窗口），按恢复策略处理。
	OutcomeRecovered Outcome = "Recovered"
	// OutcomeFenced 旧持有者/旧世代的迟到动作被 fencing 拒绝。
	OutcomeFenced Outcome = "Fenced"
	// OutcomeRequeued 本次未决（租约冲突/写回冲突/副作用失败），需重试。
	OutcomeRequeued Outcome = "Requeued"
	// OutcomeNoObject 对象已删除，无需生效。
	OutcomeNoObject Outcome = "NoObject"
	// OutcomeLegacy 未配置生效台账，走旧版幂等逻辑。
	OutcomeLegacy Outcome = "Legacy"
)

// RecoveryPolicy 规定遇到 Pending in-doubt 记录（崩溃在
// “外部动作已发出、但还没记录完成”窗口）时怎么办。
type RecoveryPolicy int

const (
	// RecoveryIdempotentRetry 携带同一 effectID 重试外部动作。
	// 前提：外部系统支持以 effectID 为幂等键去重（推荐）。
	RecoveryIdempotentRetry RecoveryPolicy = iota
	// RecoveryFailClosed 不自动重试，保持 Pending 并报错，
	// 等待人工对账（Ledger.Resolve）。杜绝“静默重复”，用于外部不幂等的场景。
	RecoveryFailClosed
)

// EffectRequest 是一次对外生效调用的输入。
type EffectRequest struct {
	Object      *store.Object
	EffectID    string // 全局幂等键：同一次生效重放始终相同
	Fingerprint string // 目标状态指纹
	Holder      string // 当前租约持有者
	Epoch       int64  // 当前领导世代
	Attempt     int    // 第几次尝试（首次为 1，in-doubt 恢复递增）
}

// EffectHandler 执行真正的对外副作用（下单、发通知等）。
// 实现应使用 req.EffectID 作为外部系统的幂等键；
// 在 fencing 已失效时控制器不会调用本方法。
type EffectHandler interface {
	Apply(ctx context.Context, req EffectRequest) error
}

// FingerprintFunc 计算对象目标状态的实质指纹：
// 只有目标状态的实质变化才应改变指纹（无关字段抖动由实现方过滤）。
type FingerprintFunc func(obj *store.Object) string

// DefaultFingerprint 默认指纹：对 Data（目标状态）做稳定序列化，
// 不含 rv/generation/status（回写状态造成的版本抖动不构成目标变化）。
func DefaultFingerprint(obj *store.Object) string {
	keys := make([]string, 0, len(obj.Data))
	for k := range obj.Data {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b []byte
	for _, k := range keys {
		b = append(b, k...)
		b = append(b, '=')
		b = append(b, obj.Data[k]...)
		b = append(b, ';')
	}
	return string(b)
}

// EffectID 由对象 key 与目标状态指纹派生出稳定的全局去重标识：
// 同一对象同一目标状态永远得到同一 ID；目标状态变化则 ID 变化。
func EffectID(key, fingerprint string) string {
	sum := sha256.Sum256([]byte(fingerprint))
	return "effect:" + key + ":" + hex.EncodeToString(sum[:8])
}

// EffectConfig 把控制器与持久化生效台账结合所需的配置，
// 由 Controller 内部持有（见 effect_reconcile.go）。
type EffectConfig struct {
	Ledger      effect.Ledger
	Effects     EffectHandler
	Fingerprint FingerprintFunc
	Recovery    RecoveryPolicy
}
