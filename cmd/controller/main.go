// 本地运行的目标状态协调控制器示例。
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

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

func main() {
	logger := log.New(os.Stdout, "[controller] ", log.LstdFlags|log.Lmicroseconds)
	st := store.NewMemoryStore()
	lm := lease.NewManager(2 * time.Second)
	ctl := reconcile.NewController("instance-1", st, lm, applyHandler{}, logger)

	obj, err := st.Create(&store.Object{Key: "demo", Data: map[string]string{"replicas": "3"}})
	if err != nil {
		logger.Fatalf("create: %v", err)
	}
	fmt.Printf("created key=%s rv=%d\n", obj.Key, obj.ResourceVersion)

	ctl.Queue.Add("demo")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctl.Run(ctx, "shard-0")

	final, _ := st.Get("demo")
	fmt.Printf("final key=%s rv=%d status=%v\n", final.Key, final.ResourceVersion, final.Status)
}
