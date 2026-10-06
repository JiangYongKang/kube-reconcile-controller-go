package effect

import (
	"errors"
	"testing"
)

func ck(key, fp string) ClaimKey { return ClaimKey{Key: key, Fingerprint: fp} }

// TestLedgerClaimDedup 同一 effectID：首次 Pending，再次 claim 报 in-doubt，
// Succeeded 之后永久报 AlreadySucceeded。
func TestLedgerClaimDedup(t *testing.T) {
	l := NewMemoryLedger()
	id := "effect:obj:fp1"

	rec, err := l.Claim(id, ck("obj", "fp1"), "inst-1", 1)
	if err != nil || rec.State != StatePending || rec.Attempts != 1 {
		t.Fatalf("首次 claim: rec=%+v err=%v", rec, err)
	}
	t.Logf("首次登记 id=%s state=%s holder=%s epoch=%d 判定=新生效意图已持久化",
		rec.ID, rec.State, rec.Holder, rec.Epoch)

	if _, err := l.Claim(id, ck("obj", "fp1"), "inst-1", 1); !errors.Is(err, ErrPendingInDoubt) {
		t.Fatalf("Pending 期间重复 claim 应报 ErrPendingInDoubt，得到 %v", err)
	}
	t.Logf("Pending 期间重复登记 判定=%v（不得当作新生效再次触发）", ErrPendingInDoubt)

	if _, err := l.Complete(id, "inst-1", 1); err != nil {
		t.Fatalf("complete: %v", err)
	}
	rec2, err := l.Claim(id, ck("obj", "fp1"), "inst-1", 1)
	if !errors.Is(err, ErrAlreadySucceeded) {
		t.Fatalf("完成后重复 claim 应报 ErrAlreadySucceeded，得到 rec=%+v err=%v", rec2, err)
	}
	t.Logf("完成后重复登记 判定=%v（同一生效对外只发生一次）", ErrAlreadySucceeded)
}

// TestLedgerFencedCompletion 换主后旧持有者用旧 epoch 的迟到完成必须被拒；
// 新持有者 reclaim 后用新 fencing 完成成功。
func TestLedgerFencedCompletion(t *testing.T) {
	l := NewMemoryLedger()
	id := "effect:obj:fp1"
	rec, err := l.Claim(id, ck("obj", "fp1"), "inst-1", 1)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("inst-1 登记 id=%s holder=%s epoch=%d", rec.ID, rec.Holder, rec.Epoch)

	// 换主：inst-2 以 epoch=2 接管并 reclaim 这条 in-doubt 记录
	rec2, err := l.Reclaim(id, "inst-2", 2)
	if err != nil {
		t.Fatalf("reclaim: %v", err)
	}
	t.Logf("inst-2 接管 Pending 记录: holder=%s epoch=%d attempts=%d", rec2.Holder, rec2.Epoch, rec2.Attempts)

	// inst-1 的迟到完成（旧 epoch=1）必须被拒
	if _, err := l.Complete(id, "inst-1", 1); !errors.Is(err, ErrFenceRejected) {
		t.Fatalf("旧持有者迟到完成应被 fencing 拒绝，得到 %v", err)
	}
	t.Logf("inst-1 旧 epoch=1 迟到完成 判定=%v（不得盖掉新实例）", ErrFenceRejected)

	got, err := l.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if got.State != StatePending {
		t.Fatalf("被拒完成不应改变状态, state=%s", got.State)
	}

	// inst-2 的完成放行
	rec3, err := l.Complete(id, "inst-2", 2)
	if err != nil || rec3.State != StateSucceeded {
		t.Fatalf("新持有者完成: rec=%+v err=%v", rec3, err)
	}
	t.Logf("inst-2 epoch=2 完成 判定=Succeeded（唯一生效者），此后任何 epoch=1 动作均无效")
}

// TestLedgerResolve Pending 记录经人工对账可 Resolve，之后按 Succeeded 去重。
func TestLedgerResolve(t *testing.T) {
	l := NewMemoryLedger()
	id := "effect:obj:fp9"
	if _, err := l.Claim(id, ck("obj", "fp9"), "inst-1", 1); err != nil {
		t.Fatal(err)
	}
	rec, err := l.Resolve(id)
	if err != nil || rec.State != StateSucceeded {
		t.Fatalf("resolve: rec=%+v err=%v", rec, err)
	}
	if _, err := l.Claim(id, ck("obj", "fp9"), "inst-2", 3); !errors.Is(err, ErrAlreadySucceeded) {
		t.Fatalf("Resolve 后应按已生效去重，得到 %v", err)
	}
	t.Logf("人工对账 Resolve 后 判定=%v（非幂等外部动作确认已执行后的兜底）", ErrAlreadySucceeded)
}
