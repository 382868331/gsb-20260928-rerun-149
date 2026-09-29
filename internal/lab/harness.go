package lab

import (
	"os"
	"path/filepath"
	"time"

	"crossledgerlab/internal/clock"
	"crossledgerlab/internal/ledger"
	"crossledgerlab/internal/message"
	"crossledgerlab/internal/node"
	"crossledgerlab/internal/protocol"
)

type Runtime struct {
	Dir         string
	RefClock    *clock.SimClock
	ClockA      *clock.SimClock
	ClockB      *clock.SimClock
	Network     *message.Network
	LedgerA     *ledger.Ledger
	LedgerB     *ledger.Ledger
	NodeA       *node.Node
	NodeB       *node.Node
	Coordinator *protocol.Coordinator
}

func NewRuntime(dir string, start time.Time, offsetA, offsetB time.Duration) (*Runtime, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	ref := clock.NewSimClock(start)
	clockA := clock.NewSimClock(start.Add(offsetA))
	clockB := clock.NewSimClock(start.Add(offsetB))
	net := message.NewNetwork(filepath.Join(dir, "network", "messages.json"), ref.Now)
	ledgerA, err := ledger.Open("A", filepath.Join(dir, "ledger-a"), clockA)
	if err != nil {
		return nil, err
	}
	ledgerB, err := ledger.Open("B", filepath.Join(dir, "ledger-b"), clockB)
	if err != nil {
		return nil, err
	}
	if err := ledgerA.EnsureAccount("alice", 1000); err != nil {
		return nil, err
	}
	if err := ledgerA.EnsureAccount("treasury-a", 0); err != nil {
		return nil, err
	}
	if err := ledgerB.EnsureAccount("treasury-b", 1000); err != nil {
		return nil, err
	}
	if err := ledgerB.EnsureAccount("bob", 0); err != nil {
		return nil, err
	}
	nodeA := node.New("ledger-a", ledgerA, filepath.Join(dir, "node-a"))
	nodeB := node.New("ledger-b", ledgerB, filepath.Join(dir, "node-b"))
	nodeA.Register(net)
	nodeB.Register(net)
	if err := net.Load(); err != nil {
		return nil, err
	}
	co := protocol.New(dir, ref, net, ledgerA, ledgerB)
	co.SetClockOffsets(offsetA, offsetB)
	if err := co.Load(); err != nil {
		return nil, err
	}
	return &Runtime{Dir: dir, RefClock: ref, ClockA: clockA, ClockB: clockB, Network: net, LedgerA: ledgerA, LedgerB: ledgerB, NodeA: nodeA, NodeB: nodeB, Coordinator: co}, nil
}

func RestartRuntime(dir string, start time.Time, offsetA, offsetB time.Duration) (*Runtime, error) {
	return NewRuntime(dir, start, offsetA, offsetB)
}
