package store

import (
	"sync"
	"time"
)

// MemoryStore 是基于内存的 Store 实现，线程安全。
type MemoryStore struct {
	mu   sync.RWMutex
	objs map[string]*Object
}

// NewMemoryStore 创建空的内存存储。
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{objs: make(map[string]*Object)}
}

func (s *MemoryStore) Create(obj *Object) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createLocked(obj)
}

func (s *MemoryStore) createLocked(obj *Object) (*Object, error) {
	if _, ok := s.objs[obj.Key]; ok {
		cur := s.objs[obj.Key]
		return nil, &ConflictError{
			Reason:   ReasonAlreadyExists,
			Key:      obj.Key,
			Expected: obj.ResourceVersion,
			Actual:   cur.ResourceVersion,
		}
	}
	cp := obj.DeepCopy()
	cp.ResourceVersion = 1
	cp.Generation = 1
	cp.UpdatedAt = time.Now()
	s.objs[cp.Key] = cp
	return cp.DeepCopy(), nil
}

func (s *MemoryStore) Get(key string) (*Object, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.objs[key].DeepCopy(), nil
}

func (s *MemoryStore) Update(obj *Object) (*Object, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.updateLocked(obj)
}

func (s *MemoryStore) updateLocked(obj *Object) (*Object, error) {
	cur, ok := s.objs[obj.Key]
	if !ok {
		return nil, &ConflictError{
			Reason:   ReasonNotFound,
			Key:      obj.Key,
			Expected: obj.ResourceVersion,
			Actual:   0,
		}
	}
	if obj.ResourceVersion <= 0 {
		return nil, &ConflictError{
			Reason:   ReasonVersionMissing,
			Key:      obj.Key,
			Expected: obj.ResourceVersion,
			Actual:   cur.ResourceVersion,
		}
	}
	if obj.ResourceVersion != cur.ResourceVersion {
		return nil, &ConflictError{
			Reason:   ReasonVersionStale,
			Key:      obj.Key,
			Expected: obj.ResourceVersion,
			Actual:   cur.ResourceVersion,
		}
	}
	cp := obj.DeepCopy()
	cp.ResourceVersion = cur.ResourceVersion + 1
	cp.Generation = cur.Generation + 1
	cp.UpdatedAt = time.Now()
	s.objs[cp.Key] = cp
	return cp.DeepCopy(), nil
}

func (s *MemoryStore) Delete(key string, resourceVersion int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.objs[key]
	if !ok {
		return &ConflictError{Reason: ReasonNotFound, Key: key, Expected: resourceVersion}
	}
	if resourceVersion <= 0 {
		return &ConflictError{Reason: ReasonVersionMissing, Key: key, Expected: resourceVersion, Actual: cur.ResourceVersion}
	}
	if resourceVersion != cur.ResourceVersion {
		return &ConflictError{Reason: ReasonVersionStale, Key: key, Expected: resourceVersion, Actual: cur.ResourceVersion}
	}
	delete(s.objs, key)
	return nil
}

func (s *MemoryStore) List() []*Object {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*Object, 0, len(s.objs))
	for _, o := range s.objs {
		out = append(out, o.DeepCopy())
	}
	return out
}
