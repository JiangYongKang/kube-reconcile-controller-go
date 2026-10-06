// Package effect 提供对外副作用（生效动作）的持久化去重与围栏判定，
// 保证同一次生效在重复投递、崩溃重启、多实例竞争场景下恰好触发一次。
package effect

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// State 是生效记录在日志中的状态。
type State string

const (
	// StateInFlight 已认领但尚未记录完成（崩溃窗口）。
	StateInFlight State = "InFlight"
	// StateCompleted 外部动作已触发并记录完成。
	StateCompleted State = "Completed"
)

// Decision 区分一次生效请求的判定结果。
type Decision string

const (
	// DecisionExecuted 本次真正触发了外部动作。
	DecisionExecuted Decision = "Executed"
	// DecisionSkippedDuplicate 同一生效已完成过，重复请求直接跳过。
	DecisionSkippedDuplicate Decision = "SkippedDuplicate"
	// DecisionRecoveredInFlight 发现崩溃窗口中的认领记录，
	// 按既定规则用同一幂等键重放，由外部系统去重。
	DecisionRecoveredInFlight Decision = "RecoveredInFlight"
	// DecisionRejectedStaleFencing 持有权标识已过期（分片被接管），拒绝生效。
	DecisionRejectedStaleFencing Decision = "RejectedStaleFencing"
)

// Record 是持久化的生效记录（去重判据）。
type Record struct {
	// ID 是生效的确定性标识：对象 key + 目标状态指纹，同时用作外部幂等键。
	ID string
	// Key 是对象 key。
	Key string
	// Shard 是对象所属分片（围栏高水位按分片维护）。
	Shard string
	// Fingerprint 是目标状态（剔除抖动字段后）的指纹。
	Fingerprint string
	// Holder 是发起生效的实例。
	Holder string
	// FencingToken 是发起时持有的租约版本（持有权标识）。
	FencingToken int64
	// ObjectRV 是发起时读到的对象资源版本。
	ObjectRV int64
	// State 是记录状态。
	State State
	// UpdatedAt 是记录最后更新时间。
	UpdatedAt time.Time
}

// Effect 是一次待触发的外部动作。
type Effect struct {
	// ID 同时作为发给外部系统的幂等键。
	ID      string
	Key     string
	Payload map[string]string
}

// Sink 抽象外部系统（下单、通知等）。
// 实现必须按 Effect.ID 幂等：同一 ID 重复调用只生效一次。
type Sink interface {
	Trigger(ctx context.Context, e Effect) error
}

// DecisionResult 携带判定结果与判定依据，用于日志与测试断言。
type DecisionResult struct {
	Decision Decision
	Record   Record
	Reason   string
	Err      error
}

// Journal 是生效记录的持久化日志。
// 它与对象存储、租约管理器一样独立于控制器进程存在，
// 因此控制器重启（新建 Controller）后去重判定依然成立。
type Journal struct {
	mu sync.Mutex
	// records 按生效 ID 索引全部记录。
	records map[string]*Record
	// latest 记录每个对象最近一次已完成生效的指纹，
	// 用于区分「目标状态未变的重放」与「目标状态实质变更」。
	latest map[string]string
	// maxToken 是每个分片出现过的最高围栏令牌（单调高水位），
	// 低于水位的生效请求一律判为过期拒绝。
	maxToken map[string]int64
	// now 时间来源，可注入便于测试。
	now func() time.Time
}

// NewJournal 创建空的生效日志。
func NewJournal() *Journal {
	return &Journal{
		records:  make(map[string]*Record),
		latest:   make(map[string]string),
		maxToken: make(map[string]int64),
		now:      time.Now,
	}
}

// LatestFingerprint 返回对象最近一次已完成生效的指纹；无记录返回空串。
func (j *Journal) LatestFingerprint(key string) string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.latest[key]
}

// Execute 以恰好一次语义执行生效。判定顺序：
//
//  1. 围栏高水位：FencingToken 低于该分片历史最高令牌 -> RejectedStaleFencing；
//  2. 去重：同 ID 记录已 Completed -> SkippedDuplicate；
//  3. 崩溃窗口：同 ID 记录停留在 InFlight（上次崩溃在「动作已发出、
//     完成未记录」之间）-> 用同一幂等键重放，由 Sink 去重，
//     判定为 RecoveredInFlight，绝不静默重复；
//  4. 否则认领新记录（InFlight）并推进围栏高水位。
//
// 认领后、触发前再经 fencingValid 复核持有权；触发成功才落 Completed。
func (j *Journal) Execute(ctx context.Context, rec Record, payload map[string]string, sink Sink, fencingValid func() bool) DecisionResult {
	j.mu.Lock()
	if high := j.maxToken[rec.Shard]; rec.FencingToken < high {
		j.mu.Unlock()
		return DecisionResult{
			Decision: DecisionRejectedStaleFencing,
			Record:   rec,
			Reason:   fmt.Sprintf("围栏令牌过期 token=%d < 高水位=%d shard=%s", rec.FencingToken, high, rec.Shard),
		}
	}
	decision := DecisionExecuted
	if existing, ok := j.records[rec.ID]; ok {
		switch existing.State {
		case StateCompleted:
			done := *existing // 锁内拷贝，避免解锁后读取共享记录
			j.mu.Unlock()
			return DecisionResult{
				Decision: DecisionSkippedDuplicate,
				Record:   done,
				Reason:   fmt.Sprintf("生效记录已完成 id=%s holder=%s token=%d", done.ID, done.Holder, done.FencingToken),
			}
		case StateInFlight:
			decision = DecisionRecoveredInFlight
		}
	} else {
		cp := rec
		cp.State = StateInFlight
		cp.UpdatedAt = j.now()
		j.records[rec.ID] = &cp
	}
	if rec.FencingToken > j.maxToken[rec.Shard] {
		j.maxToken[rec.Shard] = rec.FencingToken
	}
	j.mu.Unlock()

	// 触发前复核持有权：换主后旧实例迟到的动作在此被拦截。
	if fencingValid != nil && !fencingValid() {
		return DecisionResult{
			Decision: DecisionRejectedStaleFencing,
			Record:   rec,
			Reason:   fmt.Sprintf("持有权复核失败 holder=%s token=%d shard=%s", rec.Holder, rec.FencingToken, rec.Shard),
		}
	}

	if err := sink.Trigger(ctx, Effect{ID: rec.ID, Key: rec.Key, Payload: payload}); err != nil {
		return DecisionResult{
			Decision: decision,
			Record:   rec,
			Reason:   "外部动作触发失败，记录保持 InFlight 等待恢复",
			Err:      err,
		}
	}

	j.mu.Lock()
	done := j.records[rec.ID]
	done.State = StateCompleted
	done.UpdatedAt = j.now()
	j.latest[rec.Key] = rec.Fingerprint
	out := *done
	j.mu.Unlock()

	reason := fmt.Sprintf("外部动作已触发 id=%s holder=%s token=%d", rec.ID, rec.Holder, rec.FencingToken)
	if decision == DecisionRecoveredInFlight {
		reason = fmt.Sprintf("崩溃窗口恢复：以同一幂等键重放 id=%s（原 holder=%s token=%d）", rec.ID, rec.Holder, rec.FencingToken)
	}
	return DecisionResult{Decision: decision, Record: out, Reason: reason}
}

// FingerprintOf 计算目标状态的确定性指纹。
// 以 "_" 开头的键视为抖动字段（时间戳、随机后缀等），不计入指纹：
// 它们的变更不会触发再次生效，其余字段的实质变更会改变指纹。
func FingerprintOf(m map[string]string) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		if strings.HasPrefix(k, "_") {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	h := sha256.New()
	for _, k := range keys {
		fmt.Fprintf(h, "%s=%s\n", k, m[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:16]
}

// IDFor 由对象 key 与目标状态指纹推导确定性的生效 ID（幂等键）。
func IDFor(key, fingerprint string) string {
	return key + "@" + fingerprint
}
