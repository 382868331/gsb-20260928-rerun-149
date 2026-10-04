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
	fmt.Println("同一参与者 alice（余额 1000）同时发起多笔转移：demo-1=100、demo-2=250、demo-3=600、demo-4=100")
	mustBegin(rt, "demo-1", 100, time.Hour)
	mustBegin(rt, "demo-2", 250, 2*time.Hour)
	mustBegin(rt, "demo-3", 600, 3*time.Hour)
	mustBegin(rt, "demo-4", 100, time.Hour)

	fmt.Println("立即断开 ledger-b，只投递 A 的锁定消息")
	rt.Network.SetDisconnected("ledger-b", true)
	records := advance(rt, 40*time.Millisecond)
	for _, record := range records {
		if errors.Is(record.Err, ledger.ErrInsufficientFunds) {
			fmt.Printf("demo-4 被 A 拒绝：%v（前 3 笔已占用 950，剩余 50 不足以再锁 100）\n", record.Err)
		}
	}
	fmt.Printf("alice 可用余额=%d（950 被三笔并发转移占用，已锁资金不能再次使用）\n", rt.LedgerA.Balance("alice"))

	fmt.Println("协调者长时间失联：推进 90 分钟后重启，重新读取两侧实际账本核实")
	rt, err = lab.RestartRuntime(*dir, rt.RefClock.Now(), offsetA, offsetB)
	must(err)
	rt.RefClock.Advance(90 * time.Minute)
	rt.ClockA.Advance(90 * time.Minute)
	rt.ClockB.Advance(90 * time.Minute)
	rt.Network.SetDisconnected("ledger-b", true)
	printAudit(rt)

	fmt.Println("恢复 B 连接并投递积压消息；demo-1 的过期 B 锁将被拒绝")
	rt.Network.SetDisconnected("ledger-b", false)
	rt.Network.RestoreDisconnected()
	records, err = rt.Network.Pump()
	must(err)
	for _, record := range records {
		if record.Err != nil {
			fmt.Printf("过期消息 -> %s/%s 被账本拒绝：%v\n", record.Message.To, record.Message.Kind, record.Err)
		}
	}

	fmt.Println("对已过截止的 demo-1 执行超时退回")
	must(rt.Coordinator.RefundExpired("demo-1"))
	advance(rt, 35*time.Millisecond)
	printAudit(rt)

	fmt.Println("--- 分次领取演练：demo-5=100，bob 分次领取 40、30，剩余 30 到期退回 ---")
	mustBegin(rt, "demo-5", 100, 30*time.Minute)
	advance(rt, 40*time.Millisecond)

	fmt.Println("第一次分次领取 40（第一次领取不等于整笔结清）")
	_, err = rt.Coordinator.DecideClaim("demo-5", 40)
	must(err)
	advance(rt, 40*time.Millisecond)
	printClaims(rt, "demo-5")

	fmt.Println("模拟回应丢失：协调者重启，用原编号追查同一决定，不重复扣款")
	rt, err = lab.RestartRuntime(*dir, rt.RefClock.Now(), offsetA, offsetB)
	must(err)
	must(rt.Coordinator.SyncClaims("demo-5"))
	advance(rt, 40*time.Millisecond)
	fmt.Printf("重发原决定后 bob 余额=%d（仍为 40，未重复入账）\n", rt.LedgerB.Balance("bob"))

	fmt.Println("第二次分次领取 30")
	_, err = rt.Coordinator.DecideClaim("demo-5", 30)
	must(err)
	advance(rt, 40*time.Millisecond)
	printClaims(rt, "demo-5")
	printConservation(rt)

	fmt.Println("推进到截止之后，剩余 30 退回；已领取的 70 不受影响")
	advance(rt, 31*time.Minute)
	must(rt.Coordinator.RefundExpired("demo-5"))
	advance(rt, 40*time.Millisecond)
	printClaims(rt, "demo-5")
	printAudit(rt)
	printConservation(rt)
	fmt.Println("本演练只使用虚拟余额与模拟时钟，不接入任何真实账本或资产")
	runFollowup(*dir+"-followup", start)
}

// 故障交错演练：600 的转移先领取 170，第二次 220 只在 B 侧落账、A 侧断连；
// 协调者重开并越过截止，退款前必须先处理尚未确定的领取决定。
func runFollowup(dir string, start time.Time) {
	fmt.Println("--- 故障交错演练（独立数据目录）：demo-6=600，先领 170，再领 220 时 A 侧断连 ---")
	rt, err := lab.NewRuntime(dir, start, 0, 0)
	must(err)
	mustBegin(rt, "demo-6", 600, time.Hour)
	advance(rt, 40*time.Millisecond)

	fmt.Println("第一次领取 170：两侧都确认")
	_, err = rt.Coordinator.DecideClaim("demo-6", 170)
	must(err)
	advance(rt, 40*time.Millisecond)
	printSettlement(rt, "demo-6")

	fmt.Println("断开 ledger-a，第二次领取 220：只在 B 侧落账，A 侧响应丢失")
	rt.Network.SetDisconnected("ledger-a", true)
	_, err = rt.Coordinator.DecideClaim("demo-6", 220)
	must(err)
	advance(rt, 40*time.Millisecond)
	printSettlement(rt, "demo-6")

	fmt.Println("协调者失联重开并越过截止；A 侧仍断连时 220 金额未知，退款必须被拒绝")
	rt, err = lab.RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	must(err)
	rt.Network.SetDisconnected("ledger-a", true)
	advance(rt, time.Hour+time.Minute)
	if err := rt.Coordinator.RefundExpired("demo-6"); err != nil {
		fmt.Printf("退款被拒绝：%v（未知金额不能同时作为可退余额）\n", err)
	}
	printSettlement(rt, "demo-6")

	fmt.Println("恢复 A 连接：用原领取编号重发，A 侧收到迟到的 220 领取，已过截止被明确拒绝")
	rt.Network.SetDisconnected("ledger-a", false)
	rt.Network.RestoreDisconnected()
	must(rt.Coordinator.SyncClaims("demo-6"))
	advance(rt, 40*time.Millisecond)
	must(rt.Coordinator.SyncClaims("demo-6"))
	printSettlement(rt, "demo-6")

	fmt.Println("未知金额已清零，执行到期退回：A 退 430，B 退 210，各自与已领部分相加均为 600")
	must(rt.Coordinator.RefundExpired("demo-6"))
	advance(rt, 40*time.Millisecond)
	printSettlement(rt, "demo-6")
	printAudit(rt)
	printConservation(rt)
}

// printSettlement 逐步核对某笔转移的已领、待定、可退与锁定金额。
func printSettlement(rt *lab.Runtime, id string) {
	reports, err := rt.Coordinator.Audit()
	must(err)
	for _, report := range reports {
		if report.ID != id {
			continue
		}
		fmt.Printf("%s 阶段=%s 待定(未知)=%d 守恒A=%v 守恒B=%v\n", report.ID, report.Stage, report.Pending, report.ConservedA, report.ConservedB)
		fmt.Printf("  A侧 已领=%d 已退=%d 仍锁=%d；B侧 已领=%d 已退=%d 仍锁=%d\n",
			report.A.Confirmed, report.A.Refunded, report.A.Locked,
			report.B.Confirmed, report.B.Refunded, report.B.Locked)
		for _, d := range report.Decisions {
			note := d.Note
			if note == "" {
				note = "-"
			}
			fmt.Printf("  决定 %s 金额=%d A侧=%s B侧=%s 说明=%s\n", d.ClaimID, d.Amount, d.SideA, d.SideB, note)
		}
	}
}

func printClaims(rt *lab.Runtime, id string) {
	escrow, err := rt.LedgerB.Status(protocol.EscrowID(id, "b"))
	must(err)
	fmt.Printf("%s B侧状态=%s（partially_claimed=一部分已结清，claimed=整笔完全结束）已确认=%d 仍锁定=%d\n",
		id, escrow.Status, escrow.ClaimedAmount, escrow.Remaining())
	for _, record := range escrow.Claims {
		fmt.Printf("  领取记录 %s：本次=%d 已确认总额=%d 仍不确定=%d\n", record.ClaimID, record.Amount, record.Confirmed, record.Remaining)
	}
	if escrow.RefundedAmount > 0 {
		fmt.Printf("  到期退回：%d\n", escrow.RefundedAmount)
	}
}

func printConservation(rt *lab.Runtime) {
	aTotal := rt.LedgerA.TotalBalance() + rt.LedgerA.LockedTotal()
	bTotal := rt.LedgerB.TotalBalance() + rt.LedgerB.LockedTotal()
	fmt.Printf("金额守恒核对：A 余额+锁定=%d（应=1000），B 余额+锁定=%d（应=1000）\n", aTotal, bTotal)
	if aTotal != 1000 || bTotal != 1000 {
		panic("金额不守恒")
	}
}

func mustBegin(rt *lab.Runtime, id string, amount uint64, duration time.Duration) {
	_, err := rt.Coordinator.Begin(id, "alice", "bob", amount, duration)
	must(err)
}

func printAudit(rt *lab.Runtime) {
	reports, err := rt.Coordinator.Audit()
	must(err)
	fmt.Println("逐笔对账（来自两侧实际账本，而非协调者过期日志）：")
	for _, report := range reports {
		fmt.Printf("  %s 阶段=%s 总额=%d 统一截止=%s\n", report.ID, report.Stage, report.Amount, report.Deadline.Format(time.RFC3339))
		fmt.Printf("    A侧 状态=%s 仍锁=%d 动作=%s\n", report.A.Status, report.A.Locked, joinActions(report.A.Actions))
		fmt.Printf("    B侧 状态=%s 仍锁=%d 动作=%s\n", report.B.Status, report.B.Locked, joinActions(report.B.Actions))
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
	return records
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
