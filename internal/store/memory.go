package store

import (
	"context"
	"sort"
	"sync"
)

// MemoryStore 是基于内存的 Store 实现。
// ResourceVersion 全局单调递增；任何成功写入都会分配新版本。
type MemoryStore struct {
	mu      sync.Mutex
	objects map[string]*Object
	nextRV  int64
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objects: make(map[string]*Object), nextRV: 1}
}

// Get 读取对象，不存在时返回 ErrNotFound。
func (s *MemoryStore) Get(_ context.Context, key string) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	obj, ok := s.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return obj.DeepCopy(), nil
}

// Create 创建对象；已存在时返回 ConflictAlreadyExists。
// 若 Spec 非空，Generation 从 1 开始。
func (s *MemoryStore) Create(_ context.Context, obj *Object) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.objects[obj.Key]; ok {
		cur := s.objects[obj.Key]
		return nil, &ConflictError{
			Reason:   ConflictAlreadyExists,
			Key:      obj.Key,
			Expected: 0,
			Actual:   cur.ResourceVersion,
		}
	}
	stored := obj.DeepCopy()
	stored.ResourceVersion = s.nextRV
	s.nextRV++
	if stored.Spec != nil {
		stored.Generation = 1
	}
	s.objects[stored.Key] = stored
	return stored.DeepCopy(), nil
}

// Update 以乐观并发方式更新对象。
//   - expectedRV <= 0：视为缺失版本，返回 ConflictMissingVersion；
//   - expectedRV != 当前 ResourceVersion：返回 ConflictStaleVersion；
//   - 成功：Spec 变化时 Generation+1，ResourceVersion 分配新值。
func (s *MemoryStore) Update(_ context.Context, obj *Object, expectedRV int64) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.objects[obj.Key]
	if !ok {
		return nil, ErrNotFound
	}
	if expectedRV <= 0 {
		return nil, &ConflictError{
			Reason:   ConflictMissingVersion,
			Key:      obj.Key,
			Expected: expectedRV,
			Actual:   cur.ResourceVersion,
		}
	}
	if expectedRV != cur.ResourceVersion {
		return nil, &ConflictError{
			Reason:   ConflictStaleVersion,
			Key:      obj.Key,
			Expected: expectedRV,
			Actual:   cur.ResourceVersion,
		}
	}
	stored := obj.DeepCopy()
	stored.ResourceVersion = s.nextRV
	s.nextRV++
	if !specEqual(cur.Spec, stored.Spec) {
		stored.Generation = cur.Generation + 1
	} else {
		stored.Generation = cur.Generation
	}
	s.objects[stored.Key] = stored
	return stored.DeepCopy(), nil
}

// List 返回全部对象（按 Key 排序的深拷贝）。
func (s *MemoryStore) List(_ context.Context) ([]*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Object, 0, len(s.objects))
	for _, obj := range s.objects {
		out = append(out, obj.DeepCopy())
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

func specEqual(a, b map[string]any) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok || va != vb {
			return false
		}
	}
	return true
}
