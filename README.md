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

- 领取必须提交与 `hash_lock` 匹配的 secret，且不能超过本地账本截止时间。
- 退回必须等到本地账本截止时间之后。
- escrow 进入 `claimed` 后不能退回，进入 `refunded` 后不能领取。
- 相同消息 ID 重复到达时，节点直接忽略，不重复扣款或加款。
- 两侧时钟偏移会换算进各自本地 deadline；消息确认延迟和链路中断由模拟网络控制。

## 运行

```bash
go test ./...
go run ./cmd/lab -dir labdata
```

默认演练中，A 侧先锁定 100 个虚拟单位，B 侧在锁定消息到达前断开。资金仍留在 A 的 escrow 中；协调者重启后从两套实际账本恢复为 `partially_locked`。全局超时后，B 的过期锁定消息被拒绝，A 的 escrow 在截止时间后退回给 alice，最终为 `partially_refunded`。

项目不接入真实网络、真实账本或真实资产，仅用于验证协议状态机和故障演练。
