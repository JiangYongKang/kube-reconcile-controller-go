# kube-reconcile-controller-go

一个可本地运行的目标状态协调控制器：用本地内存对象存储模拟集群状态，
保证在并发变更、协调中断与实例故障场景下，最终状态可预期、可验证。

## 结构

```
pkg/store      本地对象存储：资源版本 + 乐观并发控制
pkg/lease      分片租约：多实例互斥协调与故障接管
pkg/effect     生效日志：对外副作用的持久化去重 + 围栏判定
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
- `Validate(shard, holder, version)` 复核持有权标识：holder、version 任一
  变化（换主）或租约过期后即失效，供生效动作发出前做围栏复核。

## 生效恰好一次（pkg/effect）

控制器启用 `Journal` + `Sink` 后，对外副作用（下单、发通知等）按
**恰好一次** 语义生效；两者为 nil 时退化为旧的纯状态写回行为（向后兼容）。

### 去重判据

- **生效 ID（幂等键）** = `对象key@目标状态指纹`。指纹由期望状态剔除
  抖动字段（`_` 前缀的键，如时间戳）后做确定性哈希得到，字段顺序无关。
- 生效记录持久化在 `Journal` 中（与 Store/Lease 一样独立于控制器进程），
  因此**进程重启后重放**依然能识别「这次生效已发生过」：
  - 同 ID 记录已 `Completed` → `SkippedDuplicate`，直接跳过；
  - 同 ID 记录停留在 `InFlight`（崩溃卡在「动作已发出、完成未记录」
    的中间态）→ **明确规则：用同一幂等键重放，由外部系统按幂等键去重**，
    判定为 `RecoveredInFlight`，绝不静默重复，也不静默丢失；
  - 无记录 → 认领为 `InFlight` 后触发，成功才落 `Completed`。
- `Sink`（外部系统）必须按 `Effect.ID` 幂等，这是崩溃窗口规则成立的前提。

### 换主时的过期判定（围栏）

- 租约版本即**持有权标识（围栏令牌）**，每次生效都携带
  `(对象版本 rv, 当前持有者 holder, 令牌 token)`，判定结果与依据逐条进日志。
- 双层拦截旧实例的迟到动作：
  1. `Journal` 按分片维护令牌**单调高水位**，低于水位的生效请求判为
     `RejectedStaleFencing`（即使其生效 ID 从未出现、去重拦不住）；
  2. 触发外部动作前用 `lease.Validate` 复核持有权，换主后立即失效。
- 被拒的生效直接丢弃（不重试），由新持有者收敛，任意时刻只有一个
  实例真正生效。

### 何时允许再次生效

- 目标状态**实质变更**（指纹变化）→ 生成新生效 ID，允许再生效一次；
- 仅抖动字段变化或完全重放（指纹不变）→ 跳过生效，仅按需写回状态；
- 两种情况在日志中可明确区分（`Executed` / `SkippedDuplicate` /
  `目标状态未变更` / `RejectedStaleFencing` / `RecoveredInFlight`）。

### 兼容范围

- 不设置 `Journal`/`Sink` 时控制器行为与上一版完全一致，既有用例不变；
- 对象存储、租约的语义未变，仅 `lease.Manager` 新增 `Validate` 方法；
- 生效去重不依赖对象 Status 中的额外字段，判据全部在 `Journal` 中。

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
| `TestDuplicateDeliverySkipped` / `TestConcurrentExecuteExactlyOnce` | 生效日志去重：重复请求、16 实例并发均恰好生效一次 |
| `TestCrashWindowRecovery` | 崩溃窗口（动作已发出、完成未记录）按同一幂等键恢复，不静默重复 |
| `TestStaleFencingRejected` / `TestFencingValidCallback` | 围栏高水位与持有权复核拦截旧实例迟到动作 |
| `TestChangedFingerprintReexecutes` / `TestFingerprintIgnoresVolatileFields` | 实质变更再次生效、抖动字段豁免、未变更不重复 |
| `TestValidateFencingToken` | 持有权标识（holder+version+未过期）复核规则 |
| `TestEffectExactlyOnceRedelivery` / `TestEffectRestartReplayNoDuplicate` | 控制器级：重复投递、重启重放均恰好生效一次 |
| `TestEffectCrashWindowResume` | 控制器级：崩溃窗口续跑，幂等键去重后恰好一次 |
| `TestEffectConcurrentInstances` | 4 实例并发抢占同一对象，恰好生效一次 |
| `TestStaleInstanceEffectRejected` | 故障接管后旧实例迟到的生效被判过期拒绝，未覆盖新实例结果 |
| `TestEffectDesiredChangeReexecutes` | 目标状态实质变更再次生效、未变更重放不重复 |
