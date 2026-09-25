package lease

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/internal/store"
)

// testClock 可手动推进的时钟，用于确定性测试租约过期。
type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func setup(t *testing.T) (context.Context, *testClock, *Manager) {
	t.Helper()
	clock := &testClock{now: time.Unix(1_700_000_000, 0)}
	m := NewManager(store.NewMemoryStore(), clock.Now)
	return context.Background(), clock, m
}

// 互斥：持有者未过期时，其他实例获取失败且原因可区分（ErrHeldByOther）。
func TestMutualExclusion(t *testing.T) {
	ctx, _, m := setup(t)
	ttl := 30 * time.Second

	l1, err := m.Acquire(ctx, "shard-0", "inst-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("inst-a acquired: fencing=%d expiresAt=%s", l1.Fencing, l1.ExpiresAt.Format(time.RFC3339))

	_, err = m.Acquire(ctx, "shard-0", "inst-b", ttl)
	t.Logf("inst-b acquire: err=%v (判定: 租约未过期且被他人持有)", err)
	if !errors.Is(err, ErrHeldByOther) {
		t.Fatalf("expected ErrHeldByOther, got %v", err)
	}

	// 自己重复获取 = 续租，Fencing 不变
	l1b, err := m.Acquire(ctx, "shard-0", "inst-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	if l1b.Fencing != l1.Fencing {
		t.Fatalf("self re-acquire bumped fencing: %d -> %d", l1.Fencing, l1b.Fencing)
	}
}

// 过期接管：持有者失联（时钟推进超过 TTL），其他实例接管且 Fencing 递增。
func TestTakeoverAfterExpiry(t *testing.T) {
	ctx, clock, m := setup(t)
	ttl := 30 * time.Second

	l1, err := m.Acquire(ctx, "shard-0", "inst-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	clock.Advance(ttl + time.Second) // inst-a 失联，租约过期

	l2, err := m.Acquire(ctx, "shard-0", "inst-b", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("takeover: oldHolder=inst-a fencing=%d -> newHolder=inst-b fencing=%d (判定: 过期后接管, Fencing+1)",
		l1.Fencing, l2.Fencing)
	if l2.Holder != "inst-b" || l2.Fencing != l1.Fencing+1 {
		t.Fatalf("bad takeover: %+v", l2)
	}

	// 旧持有者续租必须失败（防止失联实例恢复后继续生效）
	_, err = m.Renew(ctx, "shard-0", "inst-a", ttl)
	t.Logf("stale holder renew: err=%v (判定: 租约已易主, 旧持有者不得继续生效)", err)
	if !errors.Is(err, ErrLostLease) {
		t.Fatalf("expected ErrLostLease, got %v", err)
	}

	// 旧持有者用过期 Fencing 令牌做防双主检查必须失败
	err = m.CheckHeld(ctx, "shard-0", "inst-a", l1.Fencing)
	if !errors.Is(err, ErrLostLease) {
		t.Fatalf("expected CheckHeld to reject stale fencing, got %v", err)
	}
	if err := m.CheckHeld(ctx, "shard-0", "inst-b", l2.Fencing); err != nil {
		t.Fatalf("current holder should pass CheckHeld: %v", err)
	}
}

// 并发抢租：N 个实例同时获取同一分片，恰好一个成功，绝不双主。
func TestConcurrentAcquireSingleWinner(t *testing.T) {
	ctx, _, m := setup(t)
	ttl := 30 * time.Second

	const contenders = 32
	var wg sync.WaitGroup
	var mu sync.Mutex
	var winners []string
	heldByOther := 0

	for i := range contenders {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			holder := string(rune('a'+i%26)) + string(rune('0'+i/26))
			l, err := m.Acquire(ctx, "shard-0", holder, ttl)
			mu.Lock()
			defer mu.Unlock()
			if err == nil {
				winners = append(winners, holder)
				_ = l
			} else if errors.Is(err, ErrHeldByOther) {
				heldByOther++
			} else {
				t.Errorf("unexpected error: %v", err)
			}
		}(i)
	}
	wg.Wait()

	cur, _ := m.Current(ctx, "shard-0")
	t.Logf("contenders=%d winners=%v heldByOther=%d holder=%s fencing=%d (判定: CAS保证同一时刻只有一个实例推进租约)",
		contenders, winners, heldByOther, cur.Holder, cur.Fencing)
	if len(winners) != 1 {
		t.Fatalf("dual primary detected: winners=%v", winners)
	}
	if cur.Holder != winners[0] {
		t.Fatalf("stored holder %q != winner %q", cur.Holder, winners[0])
	}
}

// 主动释放：持有者释放后他人可立即接管，Fencing 继续递增。
func TestReleaseAndReacquire(t *testing.T) {
	ctx, _, m := setup(t)
	ttl := 30 * time.Second

	l1, _ := m.Acquire(ctx, "shard-0", "inst-a", ttl)
	if err := m.Release(ctx, "shard-0", "inst-b"); !errors.Is(err, ErrLostLease) {
		t.Fatalf("non-holder release should fail with ErrLostLease, got %v", err)
	}
	if err := m.Release(ctx, "shard-0", "inst-a"); err != nil {
		t.Fatal(err)
	}

	l2, err := m.Acquire(ctx, "shard-0", "inst-b", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("release-takeover: fencing %d -> %d (判定: 释放即过期, 接管方Fencing+1)", l1.Fencing, l2.Fencing)
	if l2.Fencing != l1.Fencing+1 {
		t.Fatalf("fencing not monotonic: %d -> %d", l1.Fencing, l2.Fencing)
	}
}

// 续租：持有者续租成功并保持 Fencing；失联过期后他人接管。
func TestRenewKeepsOwnership(t *testing.T) {
	ctx, clock, m := setup(t)
	ttl := 30 * time.Second

	l1, _ := m.Acquire(ctx, "shard-0", "inst-a", ttl)
	clock.Advance(20 * time.Second)
	l2, err := m.Renew(ctx, "shard-0", "inst-a", ttl)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("renew: fencing=%d expiry extended (判定: 持有者续租不改变Fencing)", l2.Fencing)
	if l2.Fencing != l1.Fencing {
		t.Fatalf("renew changed fencing: %d -> %d", l1.Fencing, l2.Fencing)
	}

	// 续租后未过期，他人仍不能获取
	clock.Advance(20 * time.Second) // 距续租 20s < ttl
	if _, err := m.Acquire(ctx, "shard-0", "inst-b", ttl); !errors.Is(err, ErrHeldByOther) {
		t.Fatalf("expected ErrHeldByOther after renew, got %v", err)
	}
}
