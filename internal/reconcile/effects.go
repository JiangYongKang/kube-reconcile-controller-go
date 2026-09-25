package reconcile

import (
	"context"
	"errors"
	"fmt"

	"github.com/highcumontoa/kube-reconcile-controller-go/internal/store"
)

// Effect 表示一次对外副作用，按 (对象 Key, Generation, Action) 去重，
// 保证重复投递或重启后同一目标状态只生效一次。
type Effect struct {
	Key        string
	Generation int64
	Action     string
	Detail     string
}

// 副作用记录的阶段。
const (
	phaseClaimed = "claimed" // 已认领但未完成（协调中断会停留在此状态）
	phaseDone    = "done"    // 已生效完成
)

// EffectLog 记录副作用的执行进度，用于幂等判定。
//
// 语义：Claim 原子认领一个副作用；fn 执行成功后 Complete 标记 done。
// 若进程在 Claim 之后、Complete 之前崩溃，记录停留在 claimed，
// 后续重投会重新执行 fn —— 因此 fn 必须具备业务幂等性；
// 一旦 Complete，相同 (Key, Generation, Action) 的副作用保证不再重复执行。
type EffectLog interface {
	// Claim 原子认领：首次认领返回 true；记录已存在返回 false。
	Claim(ctx context.Context, e Effect) (claimed bool, err error)
	// Complete 将已认领的副作用标记为 done。
	Complete(ctx context.Context, e Effect) error
	// Done 查询副作用是否已完成生效。
	Done(ctx context.Context, key string, generation int64, action string) (bool, error)
}

// StoreEffectLog 把副作用记录存放在对象存储中，进程重启后仍然可见。
// 认领利用存储 Create 的 AlreadyExists 冲突语义实现原子去重。
type StoreEffectLog struct {
	st store.Store
}

// NewStoreEffectLog 基于给定存储创建 EffectLog。
func NewStoreEffectLog(st store.Store) *StoreEffectLog {
	return &StoreEffectLog{st: st}
}

func effectKey(key string, generation int64, action string) string {
	return fmt.Sprintf("__effect__/%s/%d/%s", key, generation, action)
}

// Claim 通过 Create 的原子性认领副作用：创建成功即首次认领。
func (l *StoreEffectLog) Claim(ctx context.Context, e Effect) (bool, error) {
	obj := &store.Object{
		Key: effectKey(e.Key, e.Generation, e.Action),
		Status: map[string]any{
			"phase":  phaseClaimed,
			"detail": e.Detail,
		},
	}
	_, err := l.st.Create(ctx, obj)
	if err == nil {
		return true, nil
	}
	var ce *store.ConflictError
	if errors.As(err, &ce) && ce.Reason == store.ConflictAlreadyExists {
		return false, nil
	}
	return false, err
}

// Complete 把副作用记录从 claimed 推进到 done（带乐观并发重试）。
func (l *StoreEffectLog) Complete(ctx context.Context, e Effect) error {
	key := effectKey(e.Key, e.Generation, e.Action)
	for range 8 {
		cur, err := l.st.Get(ctx, key)
		if err != nil {
			return err
		}
		if cur.Status["phase"] == phaseDone {
			return nil
		}
		next := cur.DeepCopy()
		next.Status["phase"] = phaseDone
		if _, err := l.st.Update(ctx, next, cur.ResourceVersion); err != nil {
			var ce *store.ConflictError
			if errors.As(err, &ce) && ce.Reason == store.ConflictStaleVersion {
				continue // 并发推进，重读重试
			}
			return err
		}
		return nil
	}
	return fmt.Errorf("complete effect %s: too many conflicts", key)
}

// Done 查询副作用是否已完成生效。
func (l *StoreEffectLog) Done(ctx context.Context, key string, generation int64, action string) (bool, error) {
	obj, err := l.st.Get(ctx, effectKey(key, generation, action))
	if errors.Is(err, store.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return obj.Status["phase"] == phaseDone, nil
}
