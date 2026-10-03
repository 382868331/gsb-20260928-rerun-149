package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"crossledgerlab/internal/lab"
	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/message"
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

	fmt.Printf("参考时钟=%s；账本A时钟偏移=%s；账本B时钟偏移=%s\n", start.UTC().Format(time.RFC3339), offsetA, offsetB)
	fmt.Println("虚构账户：alice=1000（A侧）、treasury-b=1000（B侧）；bob 分次领取")
	mustBegin(rt, "demo-partial", 600, time.Hour)
	mustBegin(rt, "demo-full", 100, 2*time.Hour)
	advance(rt, 40*time.Millisecond)
	fmt.Printf("锁定后 alice 可用=%d（600+100 已锁，不能再次占用）\n", rt.LedgerA.Balance("alice"))

	fmt.Println("\n== 第一次分次领取 200：只确认本次金额，不视为整笔结清 ==")
	claim1, err := rt.Coordinator.ClaimPartial("demo-partial", 200)
	must(err)
	advance(rt, 40*time.Millisecond)
	printClaims(rt, "demo-partial")
	fmt.Printf("bob=%d（仅本次 200 到账）\n", rt.LedgerB.Balance("bob"))

	fmt.Println("\n== 重复确认：回应丢失后用同一 claim ID 重发，不多扣余额 ==")
	esc, err := rt.LedgerB.Claim(protocol.EscrowID("demo-partial", "b"), secretOf(rt, "demo-partial"), claim1, 200)
	must(err)
	fmt.Printf("重发 %s 返回已确认记录（本次=%d 累计=%d 剩余=%d），bob 仍=%d\n",
		claim1, esc.Claims[0].Amount, esc.Claimed, esc.Remaining(), rt.LedgerB.Balance("bob"))

	fmt.Println("\n== 第二次分次领取 150 发出时 B 断连：领取决定未知 ==")
	rt.Network.SetDisconnected("ledger-b", true)
	claim2, err := rt.Coordinator.ClaimPartial("demo-partial", 150)
	must(err)
	advance(rt, 40*time.Millisecond)
	fmt.Printf("claim %s 已发出但未确认；协调者重启后继续追查原决定\n", claim2)

	rt, err = lab.RestartRuntime(*dir, rt.RefClock.Now(), offsetA, offsetB)
	must(err)
	rt.Network.SetDisconnected("ledger-b", true)
	printAudit(rt)

	fmt.Println("恢复连接，按原 claim ID 重发（不换编号），未知领取得到确认")
	rt.Network.SetDisconnected("ledger-b", false)
	resent, err := rt.Coordinator.ResumePending("demo-partial")
	must(err)
	fmt.Printf("重发未确认决定=%v\n", resent)
	advance(rt, 40*time.Millisecond)
	printClaims(rt, "demo-partial")

	fmt.Println("\n== 推进到截止后：未知领取已确认，退回未领取余额 250 ==")
	advance(rt, time.Hour)
	must(rt.Coordinator.RefundExpired("demo-partial"))
	advance(rt, 40*time.Millisecond)
	printAudit(rt)

	fmt.Println("\n== 对照：demo-full 一次性全额领取，整笔完全结束 ==")
	must(rt.Coordinator.Claim("demo-full"))
	advance(rt, 40*time.Millisecond)

	fmt.Println("\n== 金额守恒核对 ==")
	aTotal := rt.LedgerA.Balance("alice") + rt.LedgerA.Balance("treasury-a")
	bTotal := rt.LedgerB.Balance("treasury-b") + rt.LedgerB.Balance("bob")
	fmt.Printf("A侧 alice=%d + treasury-a=%d = %d（应=1000）\n", rt.LedgerA.Balance("alice"), rt.LedgerA.Balance("treasury-a"), aTotal)
	fmt.Printf("B侧 treasury-b=%d + bob=%d = %d（应=1000）\n", rt.LedgerB.Balance("treasury-b"), rt.LedgerB.Balance("bob"), bTotal)
	if aTotal != 1000 || bTotal != 1000 {
		panic("金额不守恒")
	}
	printAudit(rt)
	fmt.Println("本演练只使用虚拟余额与模拟时钟，不接入任何真实账本或资产")
}

func mustBegin(rt *lab.Runtime, id string, amount uint64, duration time.Duration) {
	_, err := rt.Coordinator.Begin(id, "alice", "bob", amount, duration)
	must(err)
}

func secretOf(rt *lab.Runtime, id string) []byte {
	inst, err := rt.Coordinator.Get(id)
	must(err)
	return inst.Secret
}

func printClaims(rt *lab.Runtime, id string) {
	esc, err := rt.LedgerB.Status(protocol.EscrowID(id, "b"))
	must(err)
	fmt.Printf("B侧 escrow 状态=%s 已确认领取=%d 剩余=%d；分次领取记录：\n", esc.Status, esc.Claimed, esc.Remaining())
	for _, record := range esc.Claims {
		fmt.Printf("  %s 本次=%d 已确认总额=%d 仍不确定=%d\n", record.ID, record.Amount, record.ConfirmedTotal, record.Remaining)
	}
	inst, err := rt.Coordinator.Reconcile(id)
	must(err)
	fmt.Printf("协议阶段=%s（部分结清与整笔结束区分：partially_claimed 不同于 claimed/mixed_settled）\n", inst.Stage)
}

func printAudit(rt *lab.Runtime) {
	reports, err := rt.Coordinator.Audit()
	must(err)
	fmt.Println("逐笔对账（来自两侧实际账本，而非协调者过期日志）：")
	for _, report := range reports {
		fmt.Printf("  %s 阶段=%s 总额=%d 未确认领取=%v\n", report.ID, report.Stage, report.Amount, report.PendingClaims)
		fmt.Printf("    A侧 状态=%s 已领=%d 已退=%d 剩余=%d 动作=%s\n", report.A.Status, report.A.Claimed, report.A.Refunded, report.A.Remaining, joinActions(report.A.Actions))
		fmt.Printf("    B侧 状态=%s 已领=%d 已退=%d 剩余=%d 动作=%s\n", report.B.Status, report.B.Claimed, report.B.Refunded, report.B.Remaining, joinActions(report.B.Actions))
	}
}

func joinActions(actions []string) string {
	out := ""
	for i, action := range actions {
		if i > 0 {
			out += "；"
		}
		out += action
	}
	return out
}

func advance(rt *lab.Runtime, d time.Duration) []message.Record {
	rt.RefClock.Advance(d)
	rt.ClockA.Advance(d)
	rt.ClockB.Advance(d)
	records, err := rt.Network.Pump()
	must(err)
	for _, record := range records {
		if record.Err != nil && !errors.Is(record.Err, ledger.ErrDeadlinePassed) {
			fmt.Printf("消息 %s/%s 被拒绝：%v\n", record.Message.To, record.Message.Kind, record.Err)
		}
	}
	return records
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
