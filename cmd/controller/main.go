// 本地运行的目标状态协调控制器示例。
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

// printSink 示例外部系统：按幂等键去重，真正生效时打印。
type printSink struct {
	mu   sync.Mutex
	seen map[string]bool
}

func (s *printSink) Trigger(_ context.Context, e effect.Effect) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.seen == nil {
		s.seen = make(map[string]bool)
	}
	if s.seen[e.ID] {
		fmt.Printf("[sink] 幂等键 %s 已生效过，去重忽略\n", e.ID)
		return nil
	}
	s.seen[e.ID] = true
	fmt.Printf("[sink] >>> 对外生效（如下单/发通知）id=%s payload=%v\n", e.ID, e.Payload)
	return nil
}

func main() {
	logger := log.New(os.Stdout, "[controller] ", log.LstdFlags|log.Lmicroseconds)
	st := store.NewMemoryStore()
	lm := lease.NewManager(2 * time.Second)
	journal := effect.NewJournal()
	sink := &printSink{}
	ctl := reconcile.NewController("instance-1", st, lm, applyHandler{}, logger)
	ctl.Journal = journal
	ctl.Sink = sink

	obj, err := st.Create(&store.Object{Key: "demo", Data: map[string]string{"replicas": "3"}})
	if err != nil {
		logger.Fatalf("create: %v", err)
	}
	fmt.Printf("created key=%s rv=%d\n", obj.Key, obj.ResourceVersion)

	// 同一 key 重复投递：队列去重 + 生效日志去重，对外只生效一次。
	for i := 0; i < 3; i++ {
		ctl.Queue.Add("demo")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctl.Run(ctx, "shard-0")

	final, _ := st.Get("demo")
	fmt.Printf("final key=%s rv=%d status=%v\n", final.Key, final.ResourceVersion, final.Status)
}
