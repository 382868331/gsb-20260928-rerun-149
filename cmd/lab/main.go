package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"crossledgerlab/internal/lab"
	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/protocol"
)

func main() {
	dir := flag.String("dir", "labdata", "实验数据目录")
	flag.Parse()

	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	offsetA := 30 * time.Second
	offsetB := -90 * time.Second
	rt, err := lab.NewRuntime(*dir, start, offsetA, offsetB)
	must(err)

	const transferID = "demo-1"
	deadline := start.Add(time.Hour)
	fmt.Printf("参考时钟=%s；账本A时钟偏移=%s；账本B时钟偏移=%s\n", start.UTC().Format(time.RFC3339), offsetA, offsetB)
	fmt.Println("发起跨系统虚拟转移：alice 锁 100，A 到账确认延迟 10ms，B 到账确认延迟 25ms")
	inst, err := rt.Coordinator.Begin(transferID, "alice", "bob", 100, time.Hour)
	must(err)
	fmt.Printf("协议=%s；统一退款截止点=%s\n", inst.Stage, deadline.Format(time.RFC3339))

	fmt.Println("立即断开 ledger-b，只投递 A 的锁定消息")
	rt.Network.SetDisconnected("ledger-b", true)
	advance(rt, 20*time.Millisecond)
	_, err = rt.Coordinator.Reconcile(transferID)
	must(err)
	printState("A锁定/B断连", rt, transferID)

	fmt.Println("协调者重启：重新打开网络、协议实例与两套实际账本")
	rt, err = lab.RestartRuntime(*dir, rt.RefClock.Now(), offsetA, offsetB)
	must(err)
	rt.Network.SetDisconnected("ledger-b", true)
	inst, err = rt.Coordinator.Reconcile(transferID)
	must(err)
	fmt.Printf("从实际账本恢复协议状态=%s；不是只读取协调者日志\n", inst.Stage)
	fmt.Printf("100 个虚拟单位仍锁在 A；到达 %s 后可由 A 退回 alice\n", deadline.Format(time.RFC3339))

	fmt.Println("推进到超时之后，恢复 B 连接并投递积压消息")
	advance(rt, time.Hour+time.Second)
	rt.Network.SetDisconnected("ledger-b", false)
	rt.Network.RestoreDisconnected()
	records, err := rt.Network.Pump()
	must(err)
	for _, record := range records {
		if record.Err != nil {
			fmt.Printf("过期消息 %s -> %s/%s 被账本拒绝：%v\n", record.Message.From, record.Message.To, record.Message.Kind, record.Err)
		}
	}

	fmt.Println("执行超时退回；A 上的锁退回，B 从未形成锁")
	must(rt.Coordinator.RefundExpired(transferID))
	advance(rt, 35*time.Millisecond)
	inst, err = rt.Coordinator.Reconcile(transferID)
	must(err)
	printState("超时处理后", rt, transferID)
	fmt.Printf("最终协议=%s；本演练不涉及任何真实资产\n", inst.Stage)
}

func advance(rt *lab.Runtime, d time.Duration) {
	rt.RefClock.Advance(d)
	rt.ClockA.Advance(d)
	rt.ClockB.Advance(d)
	_, err := rt.Network.Pump()
	must(err)
}

func printState(label string, rt *lab.Runtime, id string) {
	a, errA := rt.LedgerA.Status(protocol.EscrowID(id, "a"))
	b, errB := rt.LedgerB.Status(protocol.EscrowID(id, "b"))
	fmt.Printf("[%s] A锁=%s，B锁=%s；alice=%d，treasury-b=%d，bob=%d；网络待投递:A=%d B=%d\n",
		label, status(a, errA), status(b, errB), rt.LedgerA.Balance("alice"), rt.LedgerB.Balance("treasury-b"), rt.LedgerB.Balance("bob"), rt.Network.PendingCount("ledger-a"), rt.Network.PendingCount("ledger-b"))
}

func status(escrow *ledger.Escrow, err error) string {
	if errors.Is(err, ledger.ErrMissing) {
		return "不存在"
	}
	if err != nil {
		return err.Error()
	}
	return escrow.Status
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
