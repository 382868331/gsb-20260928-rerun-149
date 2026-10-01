package main

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"crossledgerlab/internal/lab"
	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/message"
	"crossledgerlab/internal/node"
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

	fmt.Printf("参考时钟=%s；账本A时钟偏移=%s；账本B时钟偏移=%s（两侧截止判定各用本地时钟）\n\n",
		start.UTC().Format(time.RFC3339), offsetA, offsetB)

	// 阶段一：领取与退款相撞。race-1=50，截止点 1h。
	fmt.Println("阶段一：领取/退款相撞（race-1，50 个虚拟单位）")
	race, err := rt.Coordinator.Begin("race-1", "alice", "bob", 50, time.Hour)
	must(err)
	advance(rt, 35*time.Millisecond)
	if _, err := rt.LedgerB.Claim(protocol.EscrowID("race-1", "b"), race.Secret); err != nil {
		panic(err)
	}
	fmt.Println("B 侧在领取窗口内凭 secret 领取成功")
	advance(rt, time.Hour+time.Second)
	if _, err := rt.LedgerB.Refund(protocol.EscrowID("race-1", "b")); !errors.Is(err, ledger.ErrAlreadyFinal) {
		panic(fmt.Sprintf("撞向已领取终态的退款应被拒绝, got %v", err))
	}
	fmt.Println("越过截止时间后退款撞向 B 的已领取 escrow：ErrAlreadyFinal，bob 只入账一次")
	if _, err := rt.LedgerA.Refund(protocol.EscrowID("race-1", "a")); err != nil {
		panic(err)
	}
	fmt.Println("A 侧本地时钟（快 30s）已过截止，A 锁退回 alice；同一笔两侧形成 claimed/refunded 混合终态")
	printReport(rt, "race-1")

	// 阶段二：一笔正常完成的转移。
	fmt.Println("\n阶段二：正常转移（ok-1，100）")
	_, err = rt.Coordinator.Begin("ok-1", "alice", "bob", 100, time.Hour)
	must(err)
	advance(rt, 35*time.Millisecond)
	must(rt.Coordinator.Claim("ok-1"))
	advance(rt, 35*time.Millisecond)
	printReport(rt, "ok-1")

	// 阶段三：同一参与者并发多笔，且 B 断连形成部分锁定。
	// alice 可用余额 900：good-3 先锁 200、stuck-2 再锁 700 成功，
	// over-4 的 201 因 900 已被全部占用而无法在 A 锁定。
	fmt.Println("\n阶段三：B 断连期间 alice 同时发起 good-3=200(6h)、stuck-2=700(2h)、over-4=201(6h)")
	rt.Network.SetDisconnected("ledger-b", true)
	_, err = rt.Coordinator.Begin("good-3", "alice", "bob", 200, 6*time.Hour)
	must(err)
	advance(rt, 15*time.Millisecond)
	_, err = rt.Coordinator.Begin("stuck-2", "alice", "bob", 700, 2*time.Hour)
	must(err)
	advance(rt, 15*time.Millisecond)
	_, err = rt.Coordinator.Begin("over-4", "alice", "bob", 201, 6*time.Hour)
	must(err)
	advance(rt, 15*time.Millisecond)
	fmt.Printf("资金占用核查：alice 余额=%d 锁定=%s 可用=%d\n",
		rt.LedgerA.Balance("alice"), fmt.Sprint(rt.LedgerA.LockedBalance("alice")), rt.LedgerA.AvailableBalance("alice"))
	fmt.Println("已锁定的 900 不能再次使用：over-4 在 A 侧返回 ErrInsufficientFunds，不形成锁；B 侧锁定消息全部积压")

	// 积压期间消息重复：用新消息 ID 重发同一锁定意图。
	for _, dup := range []struct {
		id       string
		amount   uint64
		deadline time.Duration
	}{
		{"stuck-2", 700, 2 * time.Hour},
		{"good-3", 200, 6 * time.Hour},
	} {
		inst, err := rt.Coordinator.Get(dup.id)
		must(err)
		payload := node.LockPayload{
			EscrowID: protocol.EscrowID(dup.id, "b"), Transfer: dup.id,
			From: protocol.TreasuryB, To: "bob", Amount: dup.amount,
			HashLock: inst.HashLock, Deadline: inst.Deadline.Add(offsetB),
		}
		for i := 0; i < 2; i++ {
			_, err = rt.Network.Send("coordinator", "ledger-b", message.KindLock, payload, 10*time.Millisecond)
			must(err)
		}
	}
	fmt.Println("断连积压中每笔 B 锁定又重复发送 2 次（共 3 条同意图消息）")
	for _, id := range []string{"stuck-2", "good-3", "over-4"} {
		printReport(rt, id)
	}

	// 阶段四：协调者长时间失联，重启后只读真实账本逐笔对账，再决定动作。
	fmt.Println("\n阶段四：协调者失联 4 小时后以全新进程重启，重新打开两侧账本")
	advance(rt, 4*time.Hour)
	rt, err = lab.RestartRuntime(*dir, rt.RefClock.Now(), offsetA, offsetB)
	must(err)
	rt.Network.SetDisconnected("ledger-b", false)
	for _, id := range []string{"stuck-2", "good-3", "over-4"} {
		printReport(rt, id)
	}
	fmt.Println("结论按账本现状得出：stuck-2 的 A 锁已过本地截止只能退；good-3 两侧锁仍在领取窗口；over-4 两侧均无锁")
	fmt.Println("协调者不依据过期日志发款，只按报告动作执行：RecoverMissingLocks 对缺失且未过期的 good-3/B 补发锁定，跳过已过期的 stuck-2/B")
	must(rt.Coordinator.RecoverMissingLocks("stuck-2"))
	must(rt.Coordinator.RecoverMissingLocks("good-3"))
	must(rt.Coordinator.RecoverMissingLocks("over-4"))
	advance(rt, 35*time.Millisecond)

	fmt.Println("\n阶段五：恢复后投递历史积压（含重复消息），逐笔核对终态")
	rt.Network.RestoreDisconnected()
	records, err := rt.Network.Pump()
	must(err)
	for _, record := range records {
		switch {
		case errors.Is(record.Err, ledger.ErrDeadlinePassed):
			fmt.Printf("过期消息 %s 被账本拒绝：%v\n", record.Message.Kind, record.Err)
		case record.Err != nil:
			fmt.Printf("消息 %s 返回错误：%v\n", record.Message.Kind, record.Err)
		default:
			fmt.Printf("重复/积压消息 %s 幂等处理，不重复扣款加款\n", record.Message.Kind)
		}
	}

	must(rt.Coordinator.RefundExpired("stuck-2"))
	advance(rt, 35*time.Millisecond)
	fmt.Println("stuck-2：A 侧 700 退回 alice，B 从未在截止前锁定")

	must(rt.Coordinator.Claim("good-3"))
	advance(rt, 35*time.Millisecond)
	fmt.Println("good-3：领取窗口内凭 secret 两侧领取；随后的退款撞向 claimed 终态被拒绝")
	if _, err := rt.LedgerB.Refund(protocol.EscrowID("good-3", "b")); !errors.Is(err, ledger.ErrAlreadyFinal) {
		panic(fmt.Sprintf("领取后退款必须拒绝, got %v", err))
	}
	repeat := node.IDPayload{EscrowID: protocol.EscrowID("stuck-2", "a")}
	_, err = rt.Network.Send("test", "ledger-a", message.KindRefund, repeat, 0)
	must(err)
	_, err = rt.Network.Pump()
	must(err)
	fmt.Println("stuck-2 的重复退款幂等，alice 不会二次到账")
	fmt.Println("over-4 只有 B 锁定、A 从未锁定；推进到 B 本地截止之后退回 treasury-b")
	advance(rt, 2*time.Hour+5*time.Second)
	must(rt.Coordinator.RefundExpired("over-4"))
	advance(rt, 35*time.Millisecond)
	overReport, _, err := rt.Coordinator.ReconcileReport("over-4")
	must(err)
	fmt.Printf("over-4 最终阶段=%s，B 锁定=%d（退回 treasury-b）\n", overReport.Stage, overReport.LockedB)

	fmt.Println("\n==== 最终逐笔对账 ====")
	for _, id := range rt.Coordinator.AllTransfers() {
		printReport(rt, id)
	}
	fmt.Printf("\n余额：alice=%d treasury-a=%d treasury-b=%d bob=%d；本演练全程不接入真实账本或真实资产\n",
		rt.LedgerA.Balance("alice"), rt.LedgerA.Balance(protocol.TreasuryA),
		rt.LedgerB.Balance(protocol.TreasuryB), rt.LedgerB.Balance("bob"))
}

func advance(rt *lab.Runtime, d time.Duration) {
	rt.RefClock.Advance(d)
	rt.ClockA.Advance(d)
	rt.ClockB.Advance(d)
	_, err := rt.Network.Pump()
	must(err)
}

func printReport(rt *lab.Runtime, id string) {
	report, locks, err := rt.Coordinator.ReconcileReport(id)
	must(err)
	fmt.Printf("· 转移 %s 金额=%d 阶段=%s 锁住金额 A=%d B=%d\n",
		report.TransferID, report.Amount, report.Stage, report.LockedA, report.LockedB)
	for _, view := range report.Sides {
		if !view.Present {
			fmt.Printf("    - %s 侧：escrow 不存在（本地时钟 %s）\n", view.Side, view.LocalNow.Format(time.RFC3339))
			continue
		}
		window := ""
		if view.Remaining != "" {
			window = "，距截止还剩 " + view.Remaining
		}
		if view.Overdue != "" {
			window = "，已过截止 " + view.Overdue
		}
		fmt.Printf("    - %s 侧：%s 金额=%d 本地截止=%s%s\n",
			view.Side, view.Status, view.Amount, view.Deadline.Format(time.RFC3339), window)
	}
	for _, action := range report.Actions {
		window := ""
		if !action.ValidFrom.IsZero() {
			window = "，生效 " + action.ValidFrom.Format(time.RFC3339)
		}
		if !action.ValidUntil.IsZero() {
			window += "，截止 " + action.ValidUntil.Format(time.RFC3339)
		}
		fmt.Printf("    动作[%s] %s allowed=%v%s — %s\n",
			action.Side, action.Action, action.Allowed, window, action.Note)
	}
	for _, summary := range locks {
		fmt.Printf("    账本%s %s：锁定=%d 可用=%d\n", summary.Ledger, summary.Account, summary.Locked, summary.Available)
	}
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
