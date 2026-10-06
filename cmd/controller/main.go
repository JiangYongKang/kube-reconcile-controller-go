// 本地运行的目标状态协调控制器示例。
//
// 默认演示启用持久化生效台账后的 exactly-once 模式：
// 外部副作用（此处用本地“下单”模拟）携带 effectID 作为幂等键，
// 重复投递/崩溃重放/多实例竞争都不会重复下单。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/effect"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/lease"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/reconcile"
	"github.com/highcumontoa/kube-reconcile-controller-go/pkg/store"
)

// applyHandler 示例 Handler：把 spec 中的 data 幂等拷贝到 status。
type applyHandler struct{}

func (applyHandler) Reconcile(_ context.Context, obj *store.Object) (map[string]string, error) {
	status := make(map[string]string, len(obj.Data)+1)
	for k, v := range obj.Data {
		status[k] = v
	}
	status["applied"] = "true"
	return status, nil
}

// orderSystem 模拟外部下单系统：按 effectID 做服务端幂等去重。
// 真实系统里应把 effectID 作为下单接口的幂等键/请求 ID 落库。
type orderSystem struct {
	mu     sync.Mutex
	orders map[string]int
}

func (o *orderSystem) Apply(_ context.Context, req reconcile.EffectRequest) error {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.orders == nil {
		o.orders = make(map[string]int)
	}
	if o.orders[req.EffectID] > 0 {
		o.orders[req.EffectID]++ // 命中幂等键：不重复下单
		fmt.Printf("  [外部系统] effectID=%s 命中幂等键，跳过重复下单（attempt=%d）\n", req.EffectID, req.Attempt)
		return nil
	}
	o.orders[req.EffectID] = 1
	fmt.Printf("  [外部系统] 真实下单一次 sku=%s effectID=%s holder=%s epoch=%d\n",
		req.Object.Data["sku"], req.EffectID, req.Holder, req.Epoch)
	return nil
}

func main() {
	logger := log.New(os.Stdout, "[controller] ", log.LstdFlags|log.Lmicroseconds)
	st := store.NewMemoryStore()
	lm := lease.NewManager(2 * time.Second)
	orders := &orderSystem{}

	ctl := reconcile.NewController("instance-1", st, lm, applyHandler{}, logger).
		WithEffect(reconcile.EffectConfig{
			Ledger:   effect.NewMemoryLedger(),
			Effects:  orders,
			Recovery: reconcile.RecoveryIdempotentRetry, // 外部系统支持幂等键
		})

	obj, err := st.Create(&store.Object{Key: "demo", Data: map[string]string{"sku": "G1", "replicas": "3"}})
	if err != nil {
		logger.Fatalf("create: %v", err)
	}
	fmt.Printf("created key=%s rv=%d\n", obj.Key, obj.ResourceVersion)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 同一对象入队多次：队列去重 + 台账去重双重保证。
	for i := 0; i < 3; i++ {
		ctl.Queue.Add("demo")
	}
	ctl.Run(ctx, "shard-0")

	final, _ := st.Get("demo")
	fmt.Printf("final key=%s rv=%d status=%v\n", final.Key, final.ResourceVersion, final.Status)
}
