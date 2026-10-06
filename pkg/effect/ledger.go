// Package effect 提供持久化的生效台账（effect ledger），
// 为对外副作用实现跨进程、跨实例的 exactly-once 去重与 fencing 判定。
package effect

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// State 是一次生效的生命周期状态。
type State string

const (
	// StatePending 表示副作用“意图”已记录（claim 已落地），
	// 但尚未确认完成。崩溃重放时必须先处理 Pending 记录。
	StatePending State = "Pending"
	// StateSucceeded 表示副作用已确认成功，对应 effectID 永久去重。
	StateSucceeded State = "Succeeded"
)

// 台账判定错误，调用方据其分支决定跳过/重试/失败关闭。
var (
	// ErrAlreadySucceeded 该 effectID 已经生效过，本次必须直接跳过。
	ErrAlreadySucceeded = errors.New("effect already succeeded")
	// ErrPendingInDoubt 该 effectID 存在未完成记录（可能副作用已经发出），
	// 不能当作全新生效再次触发。
	ErrPendingInDoubt = errors.New("effect pending in-doubt")
)

// ClaimKey 是一次生效的归属判据：对象 + 目标状态指纹。
// 同一 (Key, Fingerprint) 的 effectID 相同；目标状态实质变化产生新指纹。
type ClaimKey struct {
	Key         string
	Fingerprint string
}

// Record 是台账中的一条持久化生效记录。
type Record struct {
	// ID 是全局去重标识：由 Key + Fingerprint 派生。
	ID string
	ClaimKey
	// Holder / Epoch 记录 claim 时的租约持有者与领导世代，
	// 是“这次生效算不算数”的 fencing 判据。
	Holder string
	Epoch  int64
	State  State
	// Attempts 记录副作用尝试次数（含 in-doubt 恢复重试）。
	Attempts  int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Ledger 是生效台账接口。生产实现应落在持久存储上；
// 本仓库提供进程内 MemoryLedger（跨控制器重启仍在，只要台账实例存活）。
type Ledger interface {
	// Claim 登记一次新生效意图。
	// 无记录：原子写入 Pending 记录并返回 (rec, nil)；
	// 已有 Succeeded：返回 (rec, ErrAlreadySucceeded)；
	// 已有 Pending：返回 (rec, ErrPendingInDoubt)。
	Claim(id string, ck ClaimKey, holder string, epoch int64) (*Record, error)
	// Reclaim 重新认领一条已存在的 Pending 记录（fencing 通过的恢复者），
	// Attempts+1；记录不存在或已 Succeeded 均为错误。
	Reclaim(id, holder string, epoch int64) (*Record, error)
	// Complete 将记录置为 Succeeded。必须携带 claim 时的 fencing 信息：
	// 当前归属（holder/epoch）不匹配时拒绝（旧实例迟到完成）。
	Complete(id, holder string, epoch int64) (*Record, error)
	// Get 读取记录；不存在返回 (nil, nil)。
	Get(id string) (*Record, error)
	// Resolve 供人工/对账把 Pending 记录标记为 Succeeded（非幂等副作用
	// 在 in-doubt 窗口确认外部实际已执行后的兜底入口）。
	Resolve(id string) (*Record, error)
}

// ErrFenceRejected 表示完成动作携带的 fencing 信息与记录归属不一致，
// 即旧持有者/旧世代的迟到完成，必须拒绝。
var ErrFenceRejected = errors.New("effect completion fenced out")

// ErrNotFound 表示台账中不存在指定记录。
var ErrNotFound = errors.New("effect record not found")

// MemoryLedger 是基于内存的线程安全台账实现。
type MemoryLedger struct {
	mu      sync.Mutex
	records map[string]*Record
}

// NewMemoryLedger 创建空的内存台账。
func NewMemoryLedger() *MemoryLedger {
	return &MemoryLedger{records: make(map[string]*Record)}
}

func (l *MemoryLedger) Claim(id string, ck ClaimKey, holder string, epoch int64) (*Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	if cur, ok := l.records[id]; ok {
		switch cur.State {
		case StateSucceeded:
			return cur.deepCopy(), ErrAlreadySucceeded
		default:
			return cur.deepCopy(), ErrPendingInDoubt
		}
	}
	rec := &Record{
		ID:        id,
		ClaimKey:  ck,
		Holder:    holder,
		Epoch:     epoch,
		State:     StatePending,
		Attempts:  1,
		CreatedAt: now,
		UpdatedAt: now,
	}
	l.records[id] = rec
	return rec.deepCopy(), nil
}

func (l *MemoryLedger) Reclaim(id, holder string, epoch int64) (*Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.records[id]
	if !ok {
		return nil, fmt.Errorf("%w: id=%s", ErrNotFound, id)
	}
	if cur.State == StateSucceeded {
		return cur.deepCopy(), ErrAlreadySucceeded
	}
	cur.Holder = holder
	cur.Epoch = epoch
	cur.Attempts++
	cur.UpdatedAt = time.Now()
	return cur.deepCopy(), nil
}

func (l *MemoryLedger) Complete(id, holder string, epoch int64) (*Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.records[id]
	if !ok {
		return nil, fmt.Errorf("%w: id=%s", ErrNotFound, id)
	}
	if cur.State == StateSucceeded {
		// 已完成记录：同一 fencing 的重复完成按幂等成功返回；
		// 旧 holder/epoch 的迟到完成显式判废，不能被幂等吞掉。
		if cur.Holder == holder && cur.Epoch == epoch {
			return cur.deepCopy(), nil
		}
		return cur.deepCopy(), fmt.Errorf("%w: id=%s recordHolder=%s recordEpoch=%d gotHolder=%s gotEpoch=%d state=Succeeded",
			ErrFenceRejected, id, cur.Holder, cur.Epoch, holder, epoch)
	}
	// Fencing：只有记录当前归属与本次完成者完全一致才允许置成功。
	// 换主后旧持有者用旧 epoch 完成会在这里被拒。
	if cur.Holder != holder || cur.Epoch != epoch {
		return cur.deepCopy(), fmt.Errorf("%w: id=%s recordHolder=%s recordEpoch=%d gotHolder=%s gotEpoch=%d",
			ErrFenceRejected, id, cur.Holder, cur.Epoch, holder, epoch)
	}
	cur.State = StateSucceeded
	cur.UpdatedAt = time.Now()
	return cur.deepCopy(), nil
}

func (l *MemoryLedger) Get(id string) (*Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.records[id].deepCopy(), nil
}

func (l *MemoryLedger) Resolve(id string) (*Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	cur, ok := l.records[id]
	if !ok {
		return nil, fmt.Errorf("%w: id=%s", ErrNotFound, id)
	}
	cur.State = StateSucceeded
	cur.UpdatedAt = time.Now()
	return cur.deepCopy(), nil
}

func (r *Record) deepCopy() *Record {
	if r == nil {
		return nil
	}
	cp := *r
	return &cp
}
