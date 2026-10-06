# kube-reconcile-controller-go

一个可本地运行的目标状态协调控制器：用本地内存对象存储模拟集群状态，
保证在并发变更、协调中断、实例故障与**对外副作用 exactly-once** 场景下，
最终状态可预期、可验证。

## 结构

```
pkg/store      本地对象存储：资源版本 + 乐观并发控制
pkg/lease      分片租约：多实例互斥协调、故障接管与领导世代 fencing token
pkg/effect     持久化生效台账：对外副作用的跨进程去重 / in-doubt 恢复 / fencing
pkg/reconcile  协调控制器：去重队列 + 幂等可重入协调循环 + exactly-once 生效流程
cmd/controller 本地运行示例（模拟“下单”外部系统，按 effectID 幂等）
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

## 租约与 fencing（pkg/lease）

- 同一分片（shard）任一时刻最多一个持有者生效，杜绝双主：
  - 无租约或租约已过期 → 任意实例可获取/接管；
  - 持有者本人再次 `Acquire` → 续租，`Version+1`，**`Epoch` 不变**；
  - 他人持有且未过期 → 返回 `ErrHeldByOther`，协调器稍后重试；
  - **持有者更换（接管）→ `Epoch+1`**。
- `Record.Epoch` 即领导世代 fencing token：续租不推进、只在换主时推进，
  因此一个实例拿到的 `(holder, epoch)` 在它失去领导权后**永久失效**。
- `Manager.FenceOK(shard, holder, epoch)` 判定一个生效动作此刻是否算数：
  当前租约必须同时满足 holder 相同、epoch 相同、未过期；任一不匹配即拒绝。
  控制器在外部副作用**发出前**与**返回后**各校验一次，台账 `Complete`
  再做一次归属校验，三重拦截旧实例的迟到动作。
- 仅持有者可 `Release`；测试可用 `FakeClock` 手动推进时间模拟过期。

## 对外副作用 exactly-once（pkg/effect + pkg/reconcile）

旧实现只要求 `Handler` 自身幂等，无法约束“真实外部系统”的重复调用。
新增的生效台账把“这次生效”变成**可持久识别的一次事实**。

### 去重判据：effectID = 对象 key + 目标状态指纹

- 每次协调读取对象后，用 `FingerprintFunc` 对**目标状态**（默认 `obj.Data`
  的稳定序列化，不含 rv/generation/status）计算指纹：

  ```
  effectID = "effect:" + key + ":" + sha256(fingerprint)[:16]
  ```

- 台账（`effect.Ledger`）按 effectID 持久保存一条记录：
  `Pending`（意图已登记、未确认完成）或 `Succeeded`（已确认生效），
  并记录登记时的 `Holder`/`Epoch` 与尝试次数 `Attempts`。
- 同一对象同一目标状态，无论重复投递、崩溃重放还是重启，算出的
  effectID 都相同 → 命中 `Succeeded` 即 `SkippedDuplicate`，
  **外部 Apply 一次都不会再进**。判定结果与依据（holder/epoch/effectID）
  在 `ReconcileResult` 和日志中都可区分。

### 单次协调流程（启用台账后）

1. **租约**：拿到当前 `(holder, epoch)`；被他人持有 → `Requeued`，不生效。
2. **读对象 + 算 effectID**。
3. **查台账**：
   - `Succeeded` → `SkippedDuplicate`，绝不重发副作用；若对象 Status
     尚未收敛（崩溃在完成标记之后、回写之前），只幂等补写 Status；
   - `Pending` → 崩溃 in-doubt 窗口，按下面的恢复规则处理；
   - 无记录 → 原子 `Claim(Pending)` 落意图，再首次触发。
4. **外部动作双 fencing**：`Apply` 前 `FenceOK`；`Apply` 返回后再
   `FenceOK`，随后 `Ledger.Complete(holder, epoch)`。归属不匹配的完成
   返回 `effect.ErrFenceRejected`，旧持有者的迟到完成无法覆盖新结果。
5. **回写 Status**：乐观并发，冲突只重读重试，**绝不重发副作用**。

同实例对同一 key 的协调用 key 级互斥串行；跨实例互斥由租约 epoch 保证。

### 崩溃窗口（外部动作已发出、完成标记未写入）的明确判定

记录停在 `Pending`，重放时**不会**被当成新生效，而是二选一的显式策略
（`reconcile.RecoveryPolicy`）：

- `RecoveryIdempotentRetry`（推荐，要求外部系统支持以 effectID 为幂等键）：
  接管者用**同一个 effectID** 重试，外部系统识别幂等键、不重复执行业务；
  判定结果为 `Recovered`。这是把“至少一次投递”收敛为“对外恰好一次”的关键。
- `RecoveryFailClosed`（外部动作不可幂等，例如无法去重的真实扣款）：
  控制器**不自动重发**，返回 `ErrInDoubtFailClosed` 并保持 `Pending`，
  等待人工/对账通过 `Ledger.Resolve(id)` 确认外部实际状态后收口。
  杜绝“静默重复”。

### 换主时的过期判定

- 新主接管 in-doubt 记录时用新 `(holder, epoch)` 调 `Reclaim`，
  `Attempts+1`；台账记录归属随之更新。
- 旧持有者随后的迟到动作在三处都会被判废：动作前/后 `FenceOK`
  （epoch 已变）、台账 `Complete` 的归属校验（`ErrFenceRejected`）。
- 即使旧主“复活”后重新协调：新主持有未过期租约时它在租约阶段即被拒；
  若它再次拿到租约而台账已是 `Succeeded`，也只会 `SkippedDuplicate`。
- 每条判定日志都带 `holder/epoch/effectID`，可区分“谁的这次生效算数”。

### 什么情况下允许再次生效

- 指纹只覆盖目标状态的**实质内容**：默认对 `Data` 做稳定序列化，
  rv/generation 变化、Status 回写、无关字段抖动都不改变指纹 → 不重生效。
- 业务上存在抖动字段时，通过 `WithEffect(FingerprintFunc: ...)`
  自定义指纹（示例见 `TestEffectTargetChangeAllowsReapply`，忽略 `noise` 键）。
- 目标状态实质变化 → 指纹变化 → 新 effectID → 作为新的一次生效
  真正触发一次；新版本的重放同样按新 effectID 去重。
- 历史 effectID 的台账记录各自独立保留，老版本仍永久去重。

## 两种模式与兼容范围

- **不调用 `WithEffect`（默认）**：行为与旧版完全一致——去重队列、
  `Handler` 幂等、Status 比对跳过、乐观写回。原有测试全部不变。
  适合副作用本身已天然幂等（或仅做内存态收敛）的场景。
- **调用 `WithEffect`**：启用持久化台账 + fencing 的 exactly-once 流程。
  要求：① 台账放在控制器进程之外的持久层（本仓库 `MemoryLedger`
  仅模拟“跨控制器重启仍存活”的共享持久层，生产需替换为 DB/事务存储）；
  ② 对外副作用以 effectID 作为外部幂等键，或对不可幂等动作采用
  FailClosed + 人工对账。
- `NewController` 签名、`ReconcileOne(bool,error)` 签名、队列与租约 API
  均保持向后兼容；新增 `ReconcileDetailed` 返回结构化判定
  （`Outcome`/`EffectID`/`Holder`/`Epoch`/`rv 变化`/`Attempts`）。

## 本地验证

```bash
# 全部测试（含竞态检测），-v 日志逐条打印 effectID 与判定依据
go test ./... -v -race

# 多跑几轮并发/接管用例，排除调度巧合
go test ./pkg/reconcile/ -race -count=20 -run 'Concurrent|Takeover'

# 本地示例：创建对象 -> 入队 -> 协调 -> 外部系统真实下单恰好一次
go run ./cmd/controller
```

关键测试场景：

| 测试 | 覆盖点 |
| --- | --- |
| `TestUpdateConflictReasons` / `TestConcurrentUpdatesNoLostUpdate` | 冲突原因可区分；8×25 并发写 rv 精确无丢失 |
| `TestLeaseMutualExclusion` / `TestLeaseConcurrentAcquire` | 续租 epoch 不变、换主 epoch+1、16 实例并发仅 1 胜者 |
| `TestLedgerClaimDedup` / `TestLedgerFencedCompletion` / `TestLedgerResolve` | 台账去重、Pending 恢复、旧 fencing 完成被拒、人工 Resolve |
| `TestIdempotentRedelivery`（旧模式） | 重复投递 rv 只前进一次 |
| `TestInterruptedReconcileConverges`（旧模式） | 中断无部分写回，重启重放收敛 |
| `TestLeaseTakeoverBetweenControllers`（旧模式） | 双实例互斥与过期接管 |
| `TestEffectExactlyOnceRedeliveryAndRestart` | 重复投递 + 进程重启重放，外部真实生效恰好 1 次 |
| `TestEffectInDoubtCrashWindowRecovery` | “动作已发出/未记录完成”崩溃窗口，同 effectID 幂等恢复仍一次 |
| `TestEffectFailClosedPolicy` | 不可幂等动作 FailClosed，不静默重发，人工 Resolve 收口 |
| `TestEffectMultiInstanceConcurrentOnce` | 8 实例并发：1 个 Applied，其余被租约拒绝，外部仅 1 次 |
| `TestEffectTakeoverFencesLateAction` | 故障接管后旧实例迟到 Complete / 迟到协调全部判废 |
| `TestEffectTargetChangeAllowsReapply` | 实质变化再生效一次；噪声抖动/未变更重放不重复 |
| `TestEffectCrashAfterCompleteBeforeStatus` | 台账完成后、Status 回写前崩溃，重放仅补状态不重发 |
