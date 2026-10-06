package lease

import (
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLeaseMutualExclusion(t *testing.T) {
	clk := NewFakeClock(time.Now())
	m := NewManager(time.Minute)
	m.SetClock(clk)

	rec, err := m.Acquire("shard-0", "A")
	if err != nil {
		t.Fatalf("A acquire: %v", err)
	}
	t.Logf("A 获得租约: holder=%s leaseVersion=%d expiresAt=%s", rec.Holder, rec.Version, rec.ExpiresAt.Format("15:04:05"))

	// B 在租约未过期时获取 -> 必须被拒绝，防止双主
	_, err = m.Acquire("shard-0", "B")
	if !errors.Is(err, ErrHeldByOther) {
		t.Fatalf("期望 ErrHeldByOther，得到 %v", err)
	}
	t.Logf("B 获取被拒绝: 判定依据=%v（A 的租约未过期）", err)
	if m.HeldBy("shard-0", "B") {
		t.Fatal("B 不应持有租约")
	}

	// A 续租
	rec2, err := m.Acquire("shard-0", "A")
	if err != nil {
		t.Fatalf("A renew: %v", err)
	}
	t.Logf("A 续租成功: leaseVersion=%d->%d", rec.Version, rec2.Version)

	// 持有者失联（时钟推进超过 TTL）-> B 可接管
	clk.Advance(2 * time.Minute)
	rec3, err := m.Acquire("shard-0", "B")
	if err != nil {
		t.Fatalf("B takeover: %v", err)
	}
	t.Logf("租约过期后 B 接管: holder=%s leaseVersion=%d", rec3.Holder, rec3.Version)

	// 旧持有者 A 不再生效
	if m.HeldBy("shard-0", "A") {
		t.Fatal("接管后 A 不应再持有租约")
	}
	t.Logf("判定: A 已失效, B 唯一生效, 无双主")
}

// TestLeaseConcurrentAcquire 多实例并发抢同一分片，任一时刻最多一个持有者。
func TestLeaseConcurrentAcquire(t *testing.T) {
	m := NewManager(50 * time.Millisecond)
	const holders = 16
	var wg sync.WaitGroup
	wins := make(chan string, holders)

	for i := 0; i < holders; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			if _, err := m.Acquire("shard-x", id); err == nil {
				wins <- id
			}
		}(string(rune('A' + i)))
	}
	wg.Wait()
	close(wins)

	var winners []string
	for w := range wins {
		winners = append(winners, w)
	}
	t.Logf("并发抢租结果: 获胜者=%v 共%d个", winners, len(winners))
	if len(winners) != 1 {
		t.Fatalf("同一时刻应只有一个持有者，实际 %d 个: %v", len(winners), winners)
	}
	snap, _ := m.Snapshot("shard-x")
	t.Logf("最终租约: holder=%s leaseVersion=%d 判定=互斥成立", snap.Holder, snap.Version)
}

func TestLeaseRelease(t *testing.T) {
	m := NewManager(time.Minute)
	if _, err := m.Acquire("s", "A"); err != nil {
		t.Fatal(err)
	}
	m.Release("s", "B") // 非持有者释放无效
	if !m.HeldBy("s", "A") {
		t.Fatal("非持有者释放不应生效")
	}
	m.Release("s", "A")
	if m.HeldBy("s", "A") {
		t.Fatal("持有者释放后不应再持有")
	}
	if _, err := m.Acquire("s", "B"); err != nil {
		t.Fatalf("释放后 B 应能获取: %v", err)
	}
	t.Logf("释放规则验证通过: 仅持有者可释放，释放后他人可获取")
}

// TestValidateFencingToken 持有权标识复核：版本与持有者任一变化即失效。
func TestValidateFencingToken(t *testing.T) {
	clk := NewFakeClock(time.Now())
	lm := NewManager(time.Minute)
	lm.SetClock(clk)

	rec, err := lm.Acquire("shard-0", "inst-1")
	if err != nil {
		t.Fatal(err)
	}
	if !lm.Validate("shard-0", "inst-1", rec.Version) {
		t.Fatal("当前持有者的有效令牌应通过复核")
	}
	t.Logf("holder=inst-1 version=%d 复核=有效", rec.Version)

	// 换主：inst-1 失联过期，inst-2 接管，旧令牌必须失效。
	clk.Advance(2 * time.Minute)
	rec2, err := lm.Acquire("shard-0", "inst-2")
	if err != nil {
		t.Fatal(err)
	}
	if lm.Validate("shard-0", "inst-1", rec.Version) {
		t.Fatal("换主后旧实例的旧令牌应失效")
	}
	if lm.Validate("shard-0", "inst-2", rec.Version) {
		t.Fatal("新持有者持旧版本号也应失效")
	}
	if !lm.Validate("shard-0", "inst-2", rec2.Version) {
		t.Fatal("新持有者的新令牌应有效")
	}
	t.Logf("换主后 holder=inst-2 version=%d；旧令牌(inst-1,v%d)复核=失效", rec2.Version, rec.Version)

	// 租约自然过期后复核同样失效。
	clk.Advance(2 * time.Minute)
	if lm.Validate("shard-0", "inst-2", rec2.Version) {
		t.Fatal("租约过期后令牌应失效")
	}
	t.Logf("判定=持有权标识（holder+version+未过期）复核规则正确")
}
