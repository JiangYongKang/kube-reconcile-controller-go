# kube-reconcile-controller-go

一个可本地运行的目标状态协调控制器：用本地对象存储模拟集群状态，
保证并发变更、协调中断与实例故障下的最终状态可预期、可验证。

## 结构

| 包 | 职责 |
| --- | --- |
| `internal/store` | 本地对象存储：资源版本 + 乐观并发控制 |
| `internal/reconcile` | 幂等、可重入的协调控制器与副作用去重 |
| `internal/lease` | 分片租约：多实例互斥协调与故障接管 |
| `cmd/controller` | 本地演示程序（两实例 + 故障接管） |
| `test` | 端到端故障接管测试 |

## 版本语义（internal/store）

- 每个对象有两个版本：
  - `ResourceVersion`：存储分配的全局单调递增版本，**任何成功写入**都会前进；
  - `Generation`：业务语义代数，仅当 `Spec` 变化时 +1（改 `Status` 不推进）。
- `Update` 必须携带已知版本 `expectedRV`（乐观并发）：
  - `expectedRV <= 0` → 拒绝，`ConflictError{Reason: MissingVersion}`；
  - `expectedRV != 当前 ResourceVersion` → 拒绝，`ConflictError{Reason: StaleVersion}`；
  - 重复 `Create` → 拒绝，`ConflictError{Reason: AlreadyExists}`。
- 冲突原因通过 `errors.As(err, &store.ConflictError{})` 的 `Reason` 字段区分。
- 并发写入同一版本时恰好一个成功，其余收到 `StaleVersion`；
  调用方以“读取-修改-比较交换”循环重试，保证不丢失更新。

## 协调规则（internal/reconcile）

- 协调可重入：同一 key 重复投递、并发投递，结果与串行执行一致
  （进程内按 key 分段锁串行化，跨进程由租约保证单写者）。
- 副作用幂等：通过 `Context.ApplyEffect(action, detail, fn)` 执行，
  按 `(Key, Generation, Action)` 去重，记录存放在对象存储中（重启后仍可见）：
  1. `Claim`：原子认领（利用 `Create` 的 `AlreadyExists` 冲突）；
  2. 执行 `fn`；
  3. `Complete`：标记 `done`。此后相同副作用**保证不再重复执行**。
- 中断恢复：若进程在 `Claim` 之后、`Complete` 之前崩溃，记录停留在
  `claimed`，重投/重启后由当前唯一写者续跑 `fn` —— 因此 `fn` 需具备
  业务幂等性（测试模拟的中断发生在副作用生效之前，全局恰好生效一次）。
- `Status` 写回由控制器统一做 CAS 重试，冲突时重读重试，不丢失更新。
- `Spec` 变更推进 `Generation`，新一代的副作用独立生效一次，旧代不重复。

## 租约规则（internal/lease）

- 租约是存储中的普通对象（key 为 `__lease__/<shard>`），所有获取/接管
  都通过携带 `ResourceVersion` 的 CAS 更新完成，因此同一分片任一时刻
  只有一个实例能生效，**杜绝双主**。
- `Acquire`：
  - 租约不存在 → 创建，`Fencing` 从 1 开始；
  - 自己持有且未过期 → 等价续租，`Fencing` 不变；
  - 他人持有且未过期 → 拒绝，`ErrHeldByOther`；
  - 已过期（持有者失联）→ 接管，`Fencing+1`。
- `Renew`：仅当前持有者可续租；租约已易主返回 `ErrLostLease`。
- `Release`：持有者主动释放（立即过期），`Fencing` 保留并继续单调递增。
- `Fencing` 令牌随每次易主递增；副作用执行前可用
  `CheckHeld(shard, holder, fencing)` 校验，拒绝过期持有者（防双主）。

## 本地验证

```bash
# 全部单元测试与端到端测试（日志含资源版本与判定依据）
go test ./... -v

# 竞态检测
go test ./... -race

# 运行本地演示：两实例 + 租约过期接管，观察最终收敛
go run ./cmd/controller
```

测试覆盖：

- `internal/store`：版本语义、冲突原因区分、并发 CAS 写入唯一成功、
  并发读改写不丢失更新；
- `internal/reconcile`：重复投递幂等、协调中断续跑、进程重启收敛、
  并发协调同对象、Generation 推进后的新目标协调；
- `internal/lease`：互斥获取、过期接管与 Fencing 递增、并发抢租唯一胜者、
  释放后接管、续租保持所有权；
- `test`：端到端故障接管 —— 持有者失联后其他实例接管，副作用全程
  只生效一次，旧实例复活不得再生效。
