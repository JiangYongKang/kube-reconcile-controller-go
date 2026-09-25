package store

import (
	"fmt"
	"sync"
	"testing"
)

func TestUpdateConflictReasons(t *testing.T) {
	s := NewMemoryStore()
	obj, err := s.Create(&Object{Key: "a", Data: map[string]string{"v": "1"}})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	t.Logf("created key=a rv=%d", obj.ResourceVersion)

	// 缺失版本
	_, err = s.Update(&Object{Key: "a", Data: map[string]string{"v": "2"}})
	reason, ok := IsConflict(err)
	if !ok || reason != ReasonVersionMissing {
		t.Fatalf("期望 VersionMissing，得到 reason=%s err=%v", reason, err)
	}
	t.Logf("缺失版本写入被拒绝: reason=%s 判定依据=%v", reason, err)

	// 版本陈旧
	stale := obj.DeepCopy() // rv=1
	if _, err := s.Update(obj.DeepCopy()); err != nil { // rv 1 -> 2
		t.Fatalf("first update: %v", err)
	}
	_, err = s.Update(stale)
	reason, ok = IsConflict(err)
	if !ok || reason != ReasonVersionStale {
		t.Fatalf("期望 VersionStale，得到 reason=%s err=%v", reason, err)
	}
	t.Logf("陈旧版本写入被拒绝: reason=%s 判定依据=%v", reason, err)

	// 对象不存在
	_, err = s.Update(&Object{Key: "ghost", ResourceVersion: 1})
	reason, ok = IsConflict(err)
	if !ok || reason != ReasonNotFound {
		t.Fatalf("期望 NotFound，得到 reason=%s err=%v", reason, err)
	}
	t.Logf("不存在对象写入被拒绝: reason=%s 判定依据=%v", reason, err)

	// 重复创建
	_, err = s.Create(&Object{Key: "a"})
	reason, ok = IsConflict(err)
	if !ok || reason != ReasonAlreadyExists {
		t.Fatalf("期望 AlreadyExists，得到 reason=%s err=%v", reason, err)
	}
	t.Logf("重复创建被拒绝: reason=%s 判定依据=%v", reason, err)
}

// TestConcurrentUpdatesNoLostUpdate 模拟多个客户端读-改-写同一对象，
// 采用冲突重试策略，验证最终版本 = 初始版本 + 成功更新次数，无丢失更新。
func TestConcurrentUpdatesNoLostUpdate(t *testing.T) {
	s := NewMemoryStore()
	if _, err := s.Create(&Object{Key: "shared", Data: map[string]string{}}); err != nil {
		t.Fatalf("create: %v", err)
	}

	const workers = 8
	const updatesPerWorker = 25
	var wg sync.WaitGroup
	var mu sync.Mutex
	conflicts := 0

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for i := 0; i < updatesPerWorker; i++ {
				field := fmt.Sprintf("w%d-%d", id, i)
				for { // 读-改-写重试循环
					cur, err := s.Get("shared")
					if err != nil {
						t.Errorf("get: %v", err)
						return
					}
					next := cur.DeepCopy()
					next.Data[field] = "done"
					if _, err := s.Update(next); err != nil {
						if reason, ok := IsConflict(err); ok && reason == ReasonVersionStale {
							mu.Lock()
							conflicts++
							mu.Unlock()
							continue // 版本陈旧，重读重试
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

	final, err := s.Get("shared")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	wantRV := int64(1 + workers*updatesPerWorker)
	t.Logf("最终 rv=%d 期望 rv=%d 冲突重试次数=%d", final.ResourceVersion, wantRV, conflicts)
	if final.ResourceVersion != wantRV {
		t.Fatalf("丢失更新: rv=%d, 期望 %d", final.ResourceVersion, wantRV)
	}
	if len(final.Data) != workers*updatesPerWorker {
		t.Fatalf("丢失字段: len(data)=%d, 期望 %d", len(final.Data), workers*updatesPerWorker)
	}
	if conflicts == 0 {
		t.Log("提示: 本次运行未发生版本冲突（调度巧合），不影响正确性")
	}
}
