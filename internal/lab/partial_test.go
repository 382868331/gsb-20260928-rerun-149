package lab

import (
	"errors"
	"testing"
	"time"

	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/protocol"
)

func lockEscrow(t *testing.T, rt *Runtime, id string, amount uint64, deadline time.Time) []byte {
	t.Helper()
	secret := []byte("partial-secret-32-bytes-0123456789!")
	if _, err := rt.LedgerB.Lock(ledger.LockRequest{ID: id, Transfer: id, From: "treasury-b", To: "bob", Amount: amount, HashLock: protocol.HashSecret(secret), Deadline: deadline}); err != nil {
		t.Fatal(err)
	}
	return secret
}

func TestPendingClaimBlocksRefundAndLateRejectionRemainsExplainable(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("uncertain-1", "alice", "bob", 600, time.Hour); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	first, err := rt.Coordinator.DecideClaim("uncertain-1", 170)
	if err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	// 协调者未收到回应；重启后仍用原 claim ID 和消息 ID 追查。
	rt, err = RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Coordinator.SyncClaims("uncertain-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)
	inst, err := rt.Coordinator.Get("uncertain-1")
	if err != nil {
		t.Fatal(err)
	}
	if inst.Claims[0].State != protocol.ClaimStateConfirmed || inst.Claims[0].ClaimID != first.ClaimID {
		t.Fatalf("first decision = %+v, want %s confirmed", inst.Claims[0], first.ClaimID)
	}

	// 第二次 220 只让 B 投递；A 的消息在断连期间积压。
	second, err := rt.Coordinator.DecideClaim("uncertain-1", 220)
	if err != nil {
		t.Fatal(err)
	}
	rt.Network.SetDisconnected("ledger-a", true)
	advanceTest(rt, 40*time.Millisecond)

	advanceTest(rt, time.Hour+time.Second)
	rt, err = RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	rt.Network.SetDisconnected("ledger-a", true)
	if err := rt.Coordinator.SyncClaims("uncertain-1"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Coordinator.RefundExpired("uncertain-1"); !errors.Is(err, ledger.ErrPendingClaim) {
		t.Fatalf("refund pending err = %v, want ErrPendingClaim", err)
	}

	// 恢复后迟到的 A 领取被明确拒绝并持久化，不再永久停在未知。
	rt.Network.SetDisconnected("ledger-a", false)
	rt.Network.RestoreDisconnected()
	records, err := rt.Network.Pump()
	if err != nil {
		t.Fatal(err)
	}
	foundDeadlineReject := false
	for _, record := range records {
		if record.Message.ID == second.MessageID+"-a" && errors.Is(record.Err, ledger.ErrDeadlinePassed) {
			foundDeadlineReject = true
		}
	}
	if !foundDeadlineReject {
		t.Fatalf("records = %#v, want late A claim rejected by deadline", records)
	}
	if err := rt.Coordinator.SyncClaims("uncertain-1"); err != nil {
		t.Fatal(err)
	}
	// 拒绝已是明确终态：再次退款不再报冲突，拒绝侧仍锁的 220 随剩余一并处置。
	if err := rt.Coordinator.RefundExpired("uncertain-1"); err != nil {
		t.Fatalf("refund after final rejection err = %v, want nil", err)
	}
	advanceTest(rt, 40*time.Millisecond)

	// 重复恢复与退款幂等：已确认 170 不被重复领取，退款不重复入账。
	if err := rt.Coordinator.SyncClaims("uncertain-1"); err != nil {
		t.Fatal(err)
	}
	if err := rt.Coordinator.RefundExpired("uncertain-1"); err != nil {
		t.Fatalf("repeated refund err = %v, want nil（幂等）", err)
	}
	advanceTest(rt, 40*time.Millisecond)

	aEscrow, err := rt.LedgerA.Status(protocol.EscrowID("uncertain-1", "a"))
	if err != nil {
		t.Fatal(err)
	}
	bEscrow, err := rt.LedgerB.Status(protocol.EscrowID("uncertain-1", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if aEscrow.ClaimedAmount != 170 || bEscrow.ClaimedAmount != 390 {
		t.Fatalf("claimed A/B = %d/%d, want 170/390", aEscrow.ClaimedAmount, bEscrow.ClaimedAmount)
	}
	// 各侧已领与退款合计仍为 600：A 侧 170+430，B 侧 390+210。
	if aEscrow.RefundedAmount != 430 || bEscrow.RefundedAmount != 210 {
		t.Fatalf("refunded A/B = %d/%d, want 430/210", aEscrow.RefundedAmount, bEscrow.RefundedAmount)
	}
	if aEscrow.ClaimedAmount+aEscrow.RefundedAmount != 600 || bEscrow.ClaimedAmount+bEscrow.RefundedAmount != 600 {
		t.Fatalf("A/B 已领+退款 = %d/%d, want 均等于 600",
			aEscrow.ClaimedAmount+aEscrow.RefundedAmount, bEscrow.ClaimedAmount+bEscrow.RefundedAmount)
	}
	if aEscrow.Remaining() != 0 || bEscrow.Remaining() != 0 {
		t.Fatalf("remaining A/B = %d/%d, want 0/0（争议已收尾，不留锁定）", aEscrow.Remaining(), bEscrow.Remaining())
	}
	if len(aEscrow.RejectedClaims) != 1 || aEscrow.RejectedClaims[0].ClaimID != second.ClaimID || aEscrow.RejectedClaims[0].Reason != "deadline_passed" {
		t.Fatalf("A rejects = %+v, want %s deadline_passed", aEscrow.RejectedClaims, second.ClaimID)
	}
	// 不删除单侧已领取事实：B 侧 220 的领取记录仍保留，双方差异可解释。
	foundBClaim := false
	for _, record := range bEscrow.Claims {
		if record.ClaimID == second.ClaimID && record.Amount == 220 {
			foundBClaim = true
		}
	}
	if !foundBClaim {
		t.Fatalf("B claims = %+v, want 保留 %s 的 220 已领取记录", bEscrow.Claims, second.ClaimID)
	}
	// 原领取决定与其收尾结果均可查询。
	inst, err = rt.Coordinator.Get("uncertain-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(inst.Claims) != 2 {
		t.Fatalf("claims = %+v, want 两笔决定", inst.Claims)
	}
	if inst.Claims[0].State != protocol.ClaimStateConfirmed || inst.Claims[0].Amount != 170 {
		t.Fatalf("first decision = %+v, want 170 confirmed", inst.Claims[0])
	}
	second2 := inst.Claims[1]
	if second2.State != protocol.ClaimStateConflict || second2.Resolution != protocol.ResolutionRejectedSideRefunded {
		t.Fatalf("second decision = %+v, want conflict 已收尾 %s", second2, protocol.ResolutionRejectedSideRefunded)
	}
	if second2.AState != protocol.ClaimStateRejected || second2.BState != protocol.ClaimStateConfirmed {
		t.Fatalf("second decision sides = %s/%s, want rejected/confirmed", second2.AState, second2.BState)
	}

	// 重开后保留终态和可解释的双方差异。
	rt, err = RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	reports, err := rt.Coordinator.Audit()
	if err != nil {
		t.Fatal(err)
	}
	var report protocol.TransferReport
	for _, candidate := range reports {
		if candidate.ID == "uncertain-1" {
			report = candidate
		}
	}
	if report.Stage != protocol.StageRefunded {
		t.Fatalf("stage = %s, want refunded（争议已收尾的终态）", report.Stage)
	}
	if report.A.Disputed != 0 || report.B.Disputed != 0 || report.A.Pending != 0 || report.B.Pending != 0 || report.A.Refundable != 0 || report.B.Refundable != 0 {
		t.Fatalf("A/B report = %+v/%+v, want 无待定/争议/可退余额", report.A, report.B)
	}
	if total := rt.LedgerA.TotalBalance() + rt.LedgerA.LockedTotal(); total != 1000 {
		t.Fatalf("A conservation = %d, want 1000", total)
	}
	if total := rt.LedgerB.TotalBalance() + rt.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B conservation = %d, want 1000", total)
	}
}

func TestPartialClaimsTraceableAndConserved(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	secret := lockEscrow(t, rt, "part-1-b", 100, start.Add(time.Hour))

	// 第一次分次领取 40：进入 partially_claimed，而不是整笔结清。
	esc, record, err := rt.LedgerB.ClaimPartial("part-1-b", secret, "c1", 40)
	if err != nil {
		t.Fatal(err)
	}
	if esc.Status != ledger.StatusPartiallyClaimed {
		t.Fatalf("status = %s, want partially_claimed", esc.Status)
	}
	if record.Amount != 40 || record.Confirmed != 40 || record.Remaining != 60 {
		t.Fatalf("record = %+v, want 本次40/已确认40/剩余60", record)
	}

	// 重复请求与丢失后重发：相同 claimID 直接返回首次记录，不重复加款。
	again, againRecord, err := rt.LedgerB.ClaimPartial("part-1-b", secret, "c1", 40)
	if err != nil {
		t.Fatal(err)
	}
	if againRecord.Confirmed != 40 || again.ClaimedAmount != 40 {
		t.Fatalf("duplicate claim = %+v/%+v, want 仍确认 40", again, againRecord)
	}
	if got := rt.LedgerB.Balance("bob"); got != 40 {
		t.Fatalf("bob = %d, want 40（重复请求不得多扣余额）", got)
	}

	// 第二次分次领取 30。
	esc, record, err = rt.LedgerB.ClaimPartial("part-1-b", secret, "c2", 30)
	if err != nil {
		t.Fatal(err)
	}
	if record.Confirmed != 70 || record.Remaining != 30 || esc.Status != ledger.StatusPartiallyClaimed {
		t.Fatalf("second record = %+v status=%s, want 确认70/剩余30", record, esc.Status)
	}

	// 超额领取被拒绝。
	if _, _, err := rt.LedgerB.ClaimPartial("part-1-b", secret, "c3", 31); err != ledger.ErrInvalidAmount {
		t.Fatalf("over-claim err = %v, want ErrInvalidAmount", err)
	}

	// 到期后只退回未领取的 30，已确认 70 不退。
	rt.ClockB.Advance(time.Hour + time.Second)
	esc, err = rt.LedgerB.Refund("part-1-b")
	if err != nil {
		t.Fatal(err)
	}
	if esc.RefundedAmount != 30 || esc.ClaimedAmount != 70 {
		t.Fatalf("refund = %+v, want 退30已领70", esc)
	}
	if got := rt.LedgerB.Balance("treasury-b"); got != 930 {
		t.Fatalf("treasury-b = %d, want 930", got)
	}
	if got := rt.LedgerB.Balance("bob"); got != 70 {
		t.Fatalf("bob = %d, want 70", got)
	}
	if total := rt.LedgerB.TotalBalance() + rt.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B 总额 = %d, want 1000（守恒）", total)
	}
}

func TestClaimRefundDecisionBoundary(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	deadline := start.Add(time.Hour)
	secret := lockEscrow(t, rt, "bound-1-b", 100, deadline)

	// 截止前最后一刻：领取有效，退款无效。
	rt.ClockB.Advance(time.Hour - time.Second)
	if _, err := rt.LedgerB.Refund("bound-1-b"); err != ledger.ErrDeadlineNotPassed {
		t.Fatalf("early refund err = %v, want ErrDeadlineNotPassed", err)
	}
	if _, _, err := rt.LedgerB.ClaimPartial("bound-1-b", secret, "c1", 60); err != nil {
		t.Fatalf("pre-deadline claim err = %v", err)
	}

	// 截止点：领取无效，退款有效且只退剩余 40；未知领取不能悬而未决又被退款。
	rt.ClockB.Advance(time.Second)
	if _, _, err := rt.LedgerB.ClaimPartial("bound-1-b", secret, "c2", 40); err != ledger.ErrDeadlinePassed {
		t.Fatalf("at-deadline claim err = %v, want ErrDeadlinePassed", err)
	}
	esc, err := rt.LedgerB.Refund("bound-1-b")
	if err != nil {
		t.Fatal(err)
	}
	if esc.RefundedAmount != 40 {
		t.Fatalf("refunded = %d, want 40（已确认 60 不退）", esc.RefundedAmount)
	}
	if total := rt.LedgerB.TotalBalance() + rt.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B 总额 = %d, want 1000", total)
	}
}

func TestCoordinatorRestartContinuesSameClaimDecision(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	rt, err := NewRuntime(dir, start, 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.Coordinator.Begin("re-1", "alice", "bob", 100, time.Hour); err != nil {
		t.Fatal(err)
	}
	advanceTest(rt, 40*time.Millisecond)

	// 决定第一次分次领取 40，但在投递前协调者失联。
	decision, err := rt.Coordinator.DecideClaim("re-1", 40)
	if err != nil {
		t.Fatal(err)
	}

	// 重启后继续追查原决定：相同 claimID 与消息 ID，不以新编号重做。
	restarted, err := RestartRuntime(dir, rt.RefClock.Now(), 0, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := restarted.Coordinator.SyncClaims("re-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(restarted, 40*time.Millisecond)
	// 回应丢失再重发一次，仍只生效一次。
	if err := restarted.Coordinator.SyncClaims("re-1"); err != nil {
		t.Fatal(err)
	}
	advanceTest(restarted, 40*time.Millisecond)

	if got := restarted.LedgerB.Balance("bob"); got != 40 {
		t.Fatalf("bob = %d, want 40（重发不得重复入账）", got)
	}
	inst, err := restarted.Coordinator.Get("re-1")
	if err != nil {
		t.Fatal(err)
	}
	if len(inst.Claims) != 1 || inst.Claims[0].ClaimID != decision.ClaimID || !inst.Claims[0].Confirmed {
		t.Fatalf("claims = %+v, want 原编号 %s 已确认", inst.Claims, decision.ClaimID)
	}
	esc, err := restarted.LedgerB.Status(protocol.EscrowID("re-1", "b"))
	if err != nil {
		t.Fatal(err)
	}
	if esc.Status != ledger.StatusPartiallyClaimed || esc.ClaimedAmount != 40 {
		t.Fatalf("escrow = %s/%d, want partially_claimed/40", esc.Status, esc.ClaimedAmount)
	}
	if total := restarted.LedgerB.TotalBalance() + restarted.LedgerB.LockedTotal(); total != 1000 {
		t.Fatalf("B 总额 = %d, want 1000", total)
	}
}
