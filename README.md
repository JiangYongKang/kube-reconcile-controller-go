# kube-reconcile-controller-go

一个可本地运行的目标状态协调控制器：用本地内存对象存储模拟集群状态，
保证在并发变更、协调中断与实例故障场景下，最终状态可预期、可验证。

## 结构

```
pkg/store      本地对象存储：资源版本 + 乐观并发控制
pkg/lease      分片租约：多实例互斥协调与故障接管
pkg/reconcile  协调控制器：去重队列 + 幂等可重入协调循环
cmd/controller 本地运行示例
```

## 版本语义（pkg/store）

- 每个对象携带 `ResourceVersion`（简称 rv）：创建时为 1，**每次成功更新单调 +1**。
- `Generation` 随每次写回递增，用于观察目标状态代际。
- 更新/删除必须携带已知版本（乐观并发控制）：
  - `rv <= 0` → 拒绝，冲突原因 `VersionMissing`；
  - `rv != 当前版本` → 拒绝，冲突原因 `VersionStale`；
  - 对象不存在 / 重复创建 → `NotFound` / `AlreadyExists`。
- 冲突错误为 `*store.ConflictError`，可用 `store.IsConflict(err)` 取出
  可区分的 `ConflictReason`，并携带期望/实际版本号作为判定依据。
- 客户端采用「读-改-写 + 冲突重试」循环即可保证**不丢失更新**
  （见 `TestConcurrentUpdatesNoLostUpdate`：N 个并发写者后 rv 恰好 = 1 + N×M）。

## 协调规则（pkg/reconcile）

- 工作队列按 key 去重：同一对象重复投递只保留一份，不会重复协调。
- 单次协调流程：检查租约 → 读取对象（记录 rv）→ 调用业务 `Handler`
  计算期望状态 → 携带读取时的 rv 乐观写回。
- **幂等与可重入**：
  - `Handler` 返回的期望状态与当前 `Status` 一致时跳过写回，
    重复投递/重放不会产生新版本或重复副作用；
  - 写回冲突（`VersionStale`）时重读重试，不丢更新；
  - 协调中途失败时不产生部分写回，重试或进程重启后重放同一对象，
    收敛到同一目标状态（见 `TestInterruptedReconcileConverges`）。
- 协调日志逐条打印 `rv` 与判定依据（获得租约 / 已达目标状态 /
  写回成功 rv 变化 / 冲突原因 / 租约被他人持有等）。

## 租约规则（pkg/lease）

- 同一分片（shard）任一时刻最多一个持有者生效，杜绝双主：
  - 无租约或租约已过期 → 任意实例可获取/接管；
  - 持有者本人再次 `Acquire` → 续租，`leaseVersion` +1；
  - 他人持有且未过期 → 返回 `ErrHeldByOther`，协调器稍后重试。
- 持有者失联（停止续租）后，租约随 TTL 过期，其他实例可接管；
  接管结果（新 holder、leaseVersion）与冲突原因在日志中可区分。
- 仅持有者可 `Release`；测试可用 `FakeClock` 手动推进时间模拟过期。

## 本地验证

```bash
# 运行全部测试（含竞态检测），日志打印资源版本与判定依据
go test ./... -v -race

# 运行本地示例：创建对象 -> 入队 -> 协调 -> 打印最终状态
go run ./cmd/controller
```

关键测试场景：

| 测试 | 覆盖点 |
| --- | --- |
| `TestUpdateConflictReasons` | 四种冲突原因可区分，日志含期望/实际 rv |
| `TestConcurrentUpdatesNoLostUpdate` | 8 并发写者 × 25 次更新，rv 精确校验无丢失更新 |
| `TestLeaseMutualExclusion` / `TestLeaseConcurrentAcquire` | 未过期拒绝、过期接管、16 实例并发抢租仅 1 胜者 |
| `TestIdempotentRedelivery` | 重复投递去重，rv 只前进一次 |
| `TestInterruptedReconcileConverges` | 协调中断无部分写回，重启重放收敛到同一状态 |
| `TestLeaseTakeoverBetweenControllers` | 双实例互斥生效与过期接管，无双主 |
| `TestConcurrentReconcileNoLostUpdate` | 控制器回写与外部写入并发，双方更新均不丢失 |
