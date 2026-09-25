package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

func conflictReason(t *testing.T, err error) ConflictReason {
	t.Helper()
	var ce *ConflictError
	if !errors.As(err, &ce) {
		t.Fatalf("expected *ConflictError, got %v", err)
	}
	return ce.Reason
}

// 版本语义：创建/更新分配单调递增的 ResourceVersion，Spec 变更推进 Generation。
func TestVersionSemantics(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()

	obj, err := st.Create(ctx, &Object{Key: "a", Spec: map[string]any{"replicas": 1}})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("create: key=a rv=%d gen=%d", obj.ResourceVersion, obj.Generation)
	if obj.ResourceVersion != 1 || obj.Generation != 1 {
		t.Fatalf("unexpected rv=%d gen=%d", obj.ResourceVersion, obj.Generation)
	}

	// 仅改 Status：RV 前进，Generation 不变
	obj.Status = map[string]any{"ready": true}
	obj2, err := st.Update(ctx, obj, obj.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("status update: rv=%d gen=%d (判定: 仅 Status 变化, Generation 不变)", obj2.ResourceVersion, obj2.Generation)
	if obj2.ResourceVersion != 2 || obj2.Generation != 1 {
		t.Fatalf("unexpected rv=%d gen=%d", obj2.ResourceVersion, obj2.Generation)
	}

	// 改 Spec：Generation 前进
	obj2.Spec["replicas"] = 3
	obj3, err := st.Update(ctx, obj2, obj2.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("spec update: rv=%d gen=%d (判定: Spec 变化, Generation+1)", obj3.ResourceVersion, obj3.Generation)
	if obj3.Generation != 2 {
		t.Fatalf("expected gen=2, got %d", obj3.Generation)
	}
}

// 冲突原因可区分：缺失版本与陈旧版本分别返回不同 Reason。
func TestConflictReasons(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	obj, err := st.Create(ctx, &Object{Key: "a", Spec: map[string]any{"x": 1}})
	if err != nil {
		t.Fatal(err)
	}

	// 缺失版本
	err = func() error {
		next := obj.DeepCopy()
		next.Spec["x"] = 2
		_, err := st.Update(ctx, next, 0)
		return err
	}()
	reason := conflictReason(t, err)
	t.Logf("missing version: rv=%d reason=%s (判定: expectedRV<=0)", obj.ResourceVersion, reason)
	if reason != ConflictMissingVersion {
		t.Fatalf("expected MissingVersion, got %s", reason)
	}

	// 先推进一次，使手上的快照变陈旧
	cur, _ := st.Get(ctx, "a")
	cur.Spec["x"] = 2
	updated, err := st.Update(ctx, cur, cur.ResourceVersion)
	if err != nil {
		t.Fatal(err)
	}

	// 用旧版本写入 → 陈旧冲突
	stale := obj.DeepCopy()
	stale.Spec["x"] = 99
	_, err = st.Update(ctx, stale, obj.ResourceVersion)
	reason = conflictReason(t, err)
	t.Logf("stale version: expectedRV=%d actualRV=%d reason=%s (判定: expectedRV!=当前RV)",
		obj.ResourceVersion, updated.ResourceVersion, reason)
	if reason != ConflictStaleVersion {
		t.Fatalf("expected StaleVersion, got %s", reason)
	}

	// 重复创建 → AlreadyExists
	_, err = st.Create(ctx, &Object{Key: "a"})
	reason = conflictReason(t, err)
	t.Logf("duplicate create: reason=%s", reason)
	if reason != ConflictAlreadyExists {
		t.Fatalf("expected AlreadyExists, got %s", reason)
	}
}

// 并发写入：N 个协程基于同一快照并发更新，恰好一个成功，
// 其余全部收到 StaleVersion 冲突 —— 无丢失更新、无静默覆盖。
func TestConcurrentUpdates(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	base, err := st.Create(ctx, &Object{Key: "a", Spec: map[string]any{"n": 0}})
	if err != nil {
		t.Fatal(err)
	}

	const writers = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	var successes int
	conflicts := map[ConflictReason]int{}

	for i := range writers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			next := base.DeepCopy()
			next.Spec["n"] = i + 1
			_, err := st.Update(ctx, next, base.ResourceVersion)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				successes++
				return
			}
			var ce *ConflictError
			if errors.As(err, &ce) {
				conflicts[ce.Reason]++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	final, _ := st.Get(ctx, "a")
	t.Logf("concurrent writers=%d success=%d conflicts=%v finalRV=%d (判定: 同一RV的CAS写入只能有一个成功)",
		writers, successes, conflicts, final.ResourceVersion)
	if successes != 1 {
		t.Fatalf("expected exactly 1 success, got %d", successes)
	}
	if conflicts[ConflictStaleVersion] != writers-1 {
		t.Fatalf("expected %d stale conflicts, got %v", writers-1, conflicts)
	}
	if final.ResourceVersion != base.ResourceVersion+1 {
		t.Fatalf("expected rv=%d, got %d", base.ResourceVersion+1, final.ResourceVersion)
	}
}

// 并发读改写（各自带重试）：所有协程最终都能写入，计数器不丢失更新。
func TestConcurrentReadModifyWriteNoLostUpdate(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	if _, err := st.Create(ctx, &Object{Key: "counter", Status: map[string]any{"count": 0}}); err != nil {
		t.Fatal(err)
	}

	const workers = 16
	const incEach = 25
	var wg sync.WaitGroup
	for w := range workers {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < incEach; i++ {
				for {
					cur, err := st.Get(ctx, "counter")
					if err != nil {
						t.Errorf("get: %v", err)
						return
					}
					next := cur.DeepCopy()
					next.Status["count"] = cur.Status["count"].(int) + 1
					if _, err := st.Update(ctx, next, cur.ResourceVersion); err != nil {
						var ce *ConflictError
						if errors.As(err, &ce) && ce.Reason == ConflictStaleVersion {
							continue // 冲突重试
						}
						t.Errorf("update: %v", err)
						return
					}
					break
				}
			}
		}(w)
	}
	wg.Wait()

	final, _ := st.Get(ctx, "counter")
	got := final.Status["count"].(int)
	want := workers * incEach
	t.Logf("workers=%d incEach=%d finalCount=%d finalRV=%d (判定: 每次+1都基于最新RV重试, 不得丢失更新)",
		workers, incEach, got, final.ResourceVersion)
	if got != want {
		t.Fatalf("lost update: got count=%d, want %d", got, want)
	}
}

// List 返回稳定排序的深拷贝，修改返回值不影响存储。
func TestListIsolation(t *testing.T) {
	st := NewMemoryStore()
	ctx := context.Background()
	for i := 3; i >= 1; i-- {
		if _, err := st.Create(ctx, &Object{Key: fmt.Sprintf("k%d", i)}); err != nil {
			t.Fatal(err)
		}
	}
	objs, err := st.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(objs) != 3 || objs[0].Key != "k1" || objs[2].Key != "k3" {
		t.Fatalf("unexpected list order: %v", objs)
	}
	objs[0].Status = map[string]any{"mutated": true}
	again, _ := st.Get(ctx, "k1")
	if again.Status != nil {
		t.Fatal("list returned shared state")
	}
}
