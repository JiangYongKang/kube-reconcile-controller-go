package effect

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// recordingSink 模拟按幂等键去重的外部系统：
// 同一 ID 重复 Trigger 只真正生效一次，calls 记录全部调用次数。
type recordingSink struct {
	mu       sync.Mutex
	calls    int
	effected map[string]int // id -> 真正生效次数
}

func newRecordingSink() *recordingSink {
	return &recordingSink{effected: make(map[string]int)}
}

func (s *recordingSink) Trigger(_ context.Context, e Effect) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.effected[e.ID]++
	return nil
}

// realEffects 返回外部系统真正生效的总次数（按幂等键去重后）。
func (s *recordingSink) realEffects() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.effected)
}

func alwaysValid() bool { return true }

func testRecord(key, shard, fp, holder string, token int64) Record {
	return Record{
		ID:           IDFor(key, fp),
		Key:          key,
		Shard:        shard,
		Fingerprint:  fp,
		Holder:       holder,
		FencingToken: token,
		ObjectRV:     1,
	}
}

// TestDuplicateDeliverySkipped 同一生效重复请求，只真正触发一次。
func TestDuplicateDeliverySkipped(t *testing.T) {
	j := NewJournal()
	sink := newRecordingSink()
	rec := testRecord("obj", "shard-0", "fp1", "inst-1", 1)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		res := j.Execute(ctx, rec, map[string]string{"a": "1"}, sink, alwaysValid)
		t.Logf("第 %d 次请求 判定=%s 依据=%s", i+1, res.Decision, res.Reason)
		if res.Err != nil {
			t.Fatalf("execute: %v", res.Err)
		}
		if i == 0 && res.Decision != DecisionExecuted {
			t.Fatalf("首次应 Executed, 实际 %s", res.Decision)
		}
		if i > 0 && res.Decision != DecisionSkippedDuplicate {
			t.Fatalf("重复请求应 SkippedDuplicate, 实际 %s", res.Decision)
		}
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("外部应只生效 1 次, 实际 %d", n)
	}
	t.Logf("判定=重复投递去重生效，外部恰好生效 1 次")
}

// TestCrashWindowRecovery 崩溃卡在「动作已发出、完成未记录」：
// 恢复方用同一幂等键重放，由外部系统去重，不静默重复。
func TestCrashWindowRecovery(t *testing.T) {
	j := NewJournal()
	sink := newRecordingSink()
	rec := testRecord("obj", "shard-0", "fp1", "inst-1", 1)
	ctx := context.Background()

	// 模拟崩溃：直接落一条 InFlight 记录（动作已发出，完成未记录）。
	j.mu.Lock()
	r := rec
	r.State = StateInFlight
	j.records[rec.ID] = &r
	j.maxToken[rec.Shard] = rec.FencingToken
	j.mu.Unlock()
	sink.Trigger(ctx, Effect{ID: rec.ID, Key: rec.Key}) // 崩溃前已发出的那次
	t.Logf("崩溃现场：记录=InFlight，外部已收到 id=%s 但未记录完成", rec.ID)

	// 重启后重放同一生效。
	res := j.Execute(ctx, rec, map[string]string{"a": "1"}, sink, alwaysValid)
	t.Logf("重放判定=%s 依据=%s", res.Decision, res.Reason)
	if res.Decision != DecisionRecoveredInFlight {
		t.Fatalf("应判定 RecoveredInFlight, 实际 %s", res.Decision)
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("幂等键去重后外部应只生效 1 次, 实际 %d", n)
	}
	if fp := j.LatestFingerprint("obj"); fp != "fp1" {
		t.Fatalf("恢复后应记录指纹 fp1, 实际 %q", fp)
	}
	t.Logf("判定=崩溃窗口按同一幂等键重放，外部去重后恰好生效 1 次")
}

// TestStaleFencingRejected 分片被接管后，旧实例迟到的生效被高水位拒绝。
func TestStaleFencingRejected(t *testing.T) {
	j := NewJournal()
	sink := newRecordingSink()
	ctx := context.Background()

	// 新持有者以更高令牌生效成功，推高水位。
	newRec := testRecord("obj", "shard-0", "fp2", "inst-2", 7)
	res := j.Execute(ctx, newRec, nil, sink, alwaysValid)
	t.Logf("新持有者 判定=%s 依据=%s", res.Decision, res.Reason)
	if res.Decision != DecisionExecuted {
		t.Fatalf("新持有者应 Executed, 实际 %s", res.Decision)
	}

	// 旧实例迟到的生效（旧令牌、旧指纹 -> 不同 ID，不会被去重拦截，必须靠围栏拦截）。
	stale := testRecord("obj", "shard-0", "fp1", "inst-1", 3)
	res = j.Execute(ctx, stale, nil, sink, alwaysValid)
	t.Logf("旧实例迟到请求 判定=%s 依据=%s", res.Decision, res.Reason)
	if res.Decision != DecisionRejectedStaleFencing {
		t.Fatalf("应判定 RejectedStaleFencing, 实际 %s", res.Decision)
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("旧实例动作应被拦截，外部仍只生效 1 次, 实际 %d", n)
	}
	t.Logf("判定=围栏高水位拒绝旧实例迟到动作，未覆盖新实例结果")
}

// TestFencingValidCallback 持有权复核失败时（已失去租约）拒绝触发。
func TestFencingValidCallback(t *testing.T) {
	j := NewJournal()
	sink := newRecordingSink()
	rec := testRecord("obj", "shard-0", "fp1", "inst-1", 1)

	res := j.Execute(context.Background(), rec, nil, sink, func() bool { return false })
	t.Logf("判定=%s 依据=%s", res.Decision, res.Reason)
	if res.Decision != DecisionRejectedStaleFencing {
		t.Fatalf("应判定 RejectedStaleFencing, 实际 %s", res.Decision)
	}
	if n := sink.realEffects(); n != 0 {
		t.Fatalf("失去持有权时不应触发外部动作, 实际 %d 次", n)
	}
	t.Logf("判定=触发前持有权复核拦截过期实例")
}

// TestChangedFingerprintReexecutes 目标状态实质变更允许再次生效；
// 未变更的重放不重复生效。
func TestChangedFingerprintReexecutes(t *testing.T) {
	j := NewJournal()
	sink := newRecordingSink()
	ctx := context.Background()

	rec1 := testRecord("obj", "shard-0", "fp1", "inst-1", 1)
	res := j.Execute(ctx, rec1, nil, sink, alwaysValid)
	t.Logf("首次 判定=%s", res.Decision)

	// 未变更重放：指纹相同 -> 去重跳过。
	res = j.Execute(ctx, rec1, nil, sink, alwaysValid)
	t.Logf("未变更重放 判定=%s 依据=%s", res.Decision, res.Reason)
	if res.Decision != DecisionSkippedDuplicate {
		t.Fatalf("未变更重放应跳过, 实际 %s", res.Decision)
	}

	// 实质变更：指纹变化 -> 新 ID，允许再次生效。
	rec2 := testRecord("obj", "shard-0", "fp2", "inst-1", 2)
	res = j.Execute(ctx, rec2, nil, sink, alwaysValid)
	t.Logf("变更后再生效 判定=%s 依据=%s", res.Decision, res.Reason)
	if res.Decision != DecisionExecuted {
		t.Fatalf("变更后应允许再次生效, 实际 %s", res.Decision)
	}
	if n := sink.realEffects(); n != 2 {
		t.Fatalf("两次不同目标状态应各生效一次共 2 次, 实际 %d", n)
	}
	if fp := j.LatestFingerprint("obj"); fp != "fp2" {
		t.Fatalf("最新指纹应为 fp2, 实际 %q", fp)
	}
	t.Logf("判定=实质变更再次生效、未变更重放不重复，两种情况可区分")
}

// TestConcurrentExecuteExactlyOnce 多实例并发抢同一对象的生效，恰好一次。
func TestConcurrentExecuteExactlyOnce(t *testing.T) {
	j := NewJournal()
	sink := newRecordingSink()
	ctx := context.Background()

	var wg sync.WaitGroup
	decisions := make(chan Decision, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 多实例竞争：各自持有不同令牌，但同一目标状态（同 ID）。
			rec := testRecord("obj", "shard-0", "fp1", fmt.Sprintf("inst-%d", i), int64(i+1))
			res := j.Execute(ctx, rec, nil, sink, alwaysValid)
			decisions <- res.Decision
		}(i)
	}
	wg.Wait()
	close(decisions)
	for d := range decisions {
		t.Logf("并发请求判定=%s", d)
	}
	if n := sink.realEffects(); n != 1 {
		t.Fatalf("16 实例并发生效同一目标状态，外部应恰好生效 1 次, 实际 %d", n)
	}
	t.Logf("判定=多实例并发下同一生效恰好触发一次")
}

// TestFingerprintIgnoresVolatileFields 抖动字段（"_" 前缀）不改变指纹，
// 实质字段变更改变指纹。
func TestFingerprintIgnoresVolatileFields(t *testing.T) {
	base := map[string]string{"replicas": "3", "image": "v1"}
	jitter := map[string]string{"replicas": "3", "image": "v1", "_ts": "2026-10-07T00:00:00Z"}
	changed := map[string]string{"replicas": "5", "image": "v1"}

	fpBase := FingerprintOf(base)
	if FingerprintOf(jitter) != fpBase {
		t.Fatal("抖动字段不应改变指纹")
	}
	if FingerprintOf(changed) == fpBase {
		t.Fatal("实质字段变更应改变指纹")
	}
	if FingerprintOf(map[string]string{"image": "v1", "replicas": "3"}) != fpBase {
		t.Fatal("字段顺序不应影响指纹")
	}
	t.Logf("指纹=%s 判定=抖动字段豁免、实质变更可区分、顺序无关", fpBase)
}
