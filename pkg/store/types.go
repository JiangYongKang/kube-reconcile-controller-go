// Package store 提供本地对象存储，模拟集群状态，
// 支持资源版本与乐观并发控制。
package store

import (
	"errors"
	"fmt"
	"time"
)

// ConflictReason 区分写入冲突的具体原因。
type ConflictReason string

const (
	// ReasonVersionMissing 写入未携带已知版本。
	ReasonVersionMissing ConflictReason = "VersionMissing"
	// ReasonVersionStale 写入携带的版本落后于当前版本。
	ReasonVersionStale ConflictReason = "VersionStale"
	// ReasonAlreadyExists 创建的对象已存在。
	ReasonAlreadyExists ConflictReason = "AlreadyExists"
	// ReasonNotFound 更新的对象不存在。
	ReasonNotFound ConflictReason = "NotFound"
)

// ConflictError 是可区分的乐观并发冲突错误。
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

// IsConflict 判断错误是否为并发冲突，并返回冲突原因。
func IsConflict(err error) (ConflictReason, bool) {
	var ce *ConflictError
	if errors.As(err, &ce) {
		return ce.Reason, true
	}
	return "", false
}

// Object 是存储中的对象，携带资源版本。
type Object struct {
	Key             string
	ResourceVersion int64
	Generation      int64
	Data            map[string]string
	Status          map[string]string
	UpdatedAt       time.Time
}

// DeepCopy 返回对象的深拷贝，避免外部修改存储内部状态。
func (o *Object) DeepCopy() *Object {
	if o == nil {
		return nil
	}
	cp := *o
	cp.Data = copyMap(o.Data)
	cp.Status = copyMap(o.Status)
	return &cp
}

func copyMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	cp := make(map[string]string, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

// Store 是本地对象存储接口。
type Store interface {
	// Create 创建对象，初始 ResourceVersion 为 1；已存在返回冲突。
	Create(obj *Object) (*Object, error)
	// Get 读取对象；不存在返回 nil, nil。
	Get(key string) (*Object, error)
	// Update 乐观并发更新：obj.ResourceVersion 必须等于当前版本，
	// 成功后版本 +1 并返回新对象。
	Update(obj *Object) (*Object, error)
	// Delete 删除对象，需携带当前版本。
	Delete(key string, resourceVersion int64) error
	// List 返回全部对象的快照。
	List() []*Object
}
