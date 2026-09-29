# 跨系统转移协议实验库

这是一个只使用本地虚拟余额的 Go 实验库。协议状态语义独立于具体账本：适配新账本时实现 `protocol.LedgerStore`，不用改 `held`、`settled`、`refunded` 等 HTLC 状态含义。

## 数据布局

- `ledger-a/accounts.json` 与 `ledger-b/accounts.json`：各账本独立账户余额和冻结额。
- `ledger-a/transfers.json` 与 `ledger-b/transfers.json`：各账本实际 HTLC 腿状态。
- `protocol/instances.json`：协议实例、双方截止时间和协调视图。
- `protocol/messages.json`：消息记录，按消息 ID 幂等去重。

## 协议规则

- 准备阶段冻结两条独立 HTLC 腿：源账本 `alice -> settlement`，目标账本 `settlement -> bob`。
- 目标截止时间早于源截止时间，给确认延迟和两侧时钟偏差留出恢复窗口。
- 密钥正确且锁未到期时可领取；同一领取重复执行只生效一次。
- 只要任一侧已经 `settled`，退款返回冲突，领取和退回不可能都成功。
- 准备态断线时，资金仍保留在两侧账本的冻结记录中；目标侧到点后可先解锁，源侧必须等到源截止时间。
- 重启后读取两侧实际 HTLC，再重写协议实例；协调者日志只作辅助，不作为最终事实。

## 运行

```sh
go test ./...
go test -race ./...
go run ./cmd/lab
```

可执行演练会展示两侧 30 秒初始时钟偏差、30 秒确认延迟、目标侧领取后源侧断开、协调者重启恢复，以及准备态断线时资金锁定位置和可退时间。
