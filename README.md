# 跨系统转移协议实验库

这是一个只使用虚拟余额的 Go 实验项目，在两套本地模拟账本上实现 HTLC 风格的双边锁定、领取与超时退回。

## 存储隔离

- `ledger-a/accounts.json`、`ledger-b/accounts.json`：各自账本的账户余额。
- `ledger-a/escrows.json`、`ledger-b/escrows.json`：各自账本上的锁、领取、退回状态。
- `protocol/instances.json`：协议实例和跨链协调状态。
- `network/messages.json`：消息发送、延迟、投递与断连积压记录。
- `node-a/messages.json`、`node-b/messages.json`：节点已处理消息集合，重复消息只生效一次。

协议实例只是缓存和协调视图；重启后会重新查询两套实际账本中的 escrow 状态并更新协议阶段，不只相信协调者日志。

## 安全规则

- 领取必须提交与 `hash_lock` 匹配的 secret，且不能超过本地账本截止时间；接收方可分几次领取，第一次领取只确认本次金额，不视为整笔结清。
- 每笔分次领取携带稳定 claim ID 并落账为 `ClaimRecord`（本次金额、已确认总额、仍不确定的剩余部分）；重复请求或回应丢失后的重传命中同一 ID，直接返回已确认记录，不多扣余额。
- 退回必须严格晚于本地账本截止时间（`now > deadline`），且只退回剩余未领取余额；领取在 `now <= deadline` 有效，二者在决定边界上互斥，不会双重支付。
- 存在已发出但未确认的领取决定时，协调者拒绝退款（`ErrPendingClaims`）：未知领取不能一边等待确认一边被退款；重开后用持久化的原 claim ID 继续追查（`ResumePending`），不以新编号重做可能已发生的效果。
- escrow 终态区分：`claimed`（整笔完全领取）、`refunded`（整笔退回）、`mixed_settled`（部分领取+余额退回，已终态）；`partially_claimed` 表示只结清了一部分。
- 相同消息 ID 重复到达时，节点直接忽略，不重复扣款或加款。
- 两侧时钟偏移会换算进各自本地 deadline；消息确认延迟和链路中断由模拟网络控制。
- 同一参与者的多笔转移可并发进行；锁定时立即从可用余额中扣减，已锁资金不能再次用于另一笔，余额不足的新锁定会被拒绝。
- 领取与退款在截止点附近交错时，账本互斥与剩余金额核对保证只形成一个有效终态，不会双重支付。

## 逐笔对账

`Coordinator.Audit()` 重新读取两侧实际账本（而非协调者过期日志），对每笔进行中的转移给出：

- 两侧 escrow 状态与仍被锁住的金额；
- 当前可采取的动作（`claim`/`refund`/终态）及其本地时限；
- 协调者长时间失联重启后，先用它核实再继续发款。

## 运行

```bash
go test ./...
go run ./cmd/lab -dir labdata
```

默认演练中，alice 发起 demo-partial（600）与 demo-full（100）。demo-partial 先分次领取 200，再演示同一 claim ID 重复确认不多扣；第二次领取 150 发出时 B 断连，协调者重启后按原 claim ID 重发确认；到期后未领取余额 250 退回，进入 `mixed_settled`。demo-full 一次性全额领取进入 `claimed`，对照部分结清与整笔完全结束。结尾核对两侧账本金额守恒（各 1000）。

项目不接入真实网络、真实账本或真实资产，仅用于验证协议状态机和故障演练。
