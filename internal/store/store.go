// Package store 提供本地对象存储，模拟集群状态，
// 支持资源版本与乐观并发控制。
package store

import (
	"context"
	"errors"
	"fmt"
)

// ConflictReason 区分写入冲突的具体原因。
type ConflictReason string

const (
	ConflictStaleVersion   ConflictReason = "StaleVersion"   // 携带的版本落后于当前版本
	ConflictMissingVersion ConflictReason = "MissingVersion" // 更新未携带已知版本
	ConflictAlreadyExists  ConflictReason = "AlreadyExists"  // 创建时对象已存在
)

// ConflictError 表示一次乐观并发冲突，Reason 可用于区分冲突类型。
type ConflictError struct {
	Reason   ConflictReason
	Key      string
	Expected int64
	Actual   int64
}

func (e *ConflictError) Error() string {
	return fmt.Sprintf("conflict key=%q reason=%s expectedRV=%d actualRV=%d",
		e.Key, e.Reason, e.Expected, e.Actual)
}

// ErrNotFound 表示对象不存在。
var ErrNotFound = errors.New("object not found")

// Object 是受管对象，ResourceVersion 由存储单调分配。
type Object struct {
	Key             string
	Generation      int64 // Spec 变更代数，业务语义版本
	ResourceVersion int64 // 存储分配的全局单调版本
	Spec            map[string]any
	Status          map[string]any
}

// DeepCopy 返回对象的深拷贝，避免调用方共享内部 map。
func (o *Object) DeepCopy() *Object {
	cp := *o
	cp.Spec = cloneMap(o.Spec)
	cp.Status = cloneMap(o.Status)
	return &cp
}

func cloneMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// Store 是对象存储接口。Update 必须携带已知版本（expectedRV），
// 版本陈旧或缺失时返回 *ConflictError。
type Store interface {
	Get(ctx context.Context, key string) (*Object, error)
	Create(ctx context.Context, obj *Object) (*Object, error)
	Update(ctx context.Context, obj *Object, expectedRV int64) (*Object, error)
	List(ctx context.Context) ([]*Object, error)
}
