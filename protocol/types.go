package protocol

import "time"

type TransferStatus string

const (
	TransferMissing  TransferStatus = "missing"
	TransferHeld     TransferStatus = "held"
	TransferSettled  TransferStatus = "settled"
	TransferRefunded TransferStatus = "refunded"
)

type ProtocolStatus string

const (
	StatusProposed   ProtocolStatus = "proposed"
	StatusPrepared   ProtocolStatus = "prepared"
	StatusCommitted  ProtocolStatus = "committed"
	StatusAborted    ProtocolStatus = "aborted"
	StatusRefunded   ProtocolStatus = "refunded"
	StatusRecovering ProtocolStatus = "recovering"
)

type Side string

const (
	Source      Side = "source"
	Destination Side = "destination"
)

type Amount uint64

type Account struct {
	Name    string `json:"name"`
	Balance Amount `json:"balance"`
	Held    Amount `json:"held"`
}

type Transfer struct {
	ID         string         `json:"id"`
	Ledger     string         `json:"ledger"`
	From       string         `json:"from"`
	To         string         `json:"to"`
	Amount     Amount         `json:"amount"`
	LockUntil  time.Time      `json:"lock_until"`
	SecretHash string         `json:"secret_hash"`
	Secret     string         `json:"secret,omitempty"`
	Status     TransferStatus `json:"status"`
	PreparedAt time.Time      `json:"prepared_at"`
	SettledAt  time.Time      `json:"settled_at,omitempty"`
	RefundedAt time.Time      `json:"refunded_at,omitempty"`
}

type Instance struct {
	ID             string         `json:"id"`
	SourceLedger   string         `json:"source_ledger"`
	DestLedger     string         `json:"dest_ledger"`
	Sender         string         `json:"sender"`
	Receiver       string         `json:"receiver"`
	Amount         Amount         `json:"amount"`
	SecretHash     string         `json:"secret_hash"`
	SourceDeadline time.Time      `json:"source_deadline"`
	DestDeadline   time.Time      `json:"dest_deadline"`
	Status         ProtocolStatus `json:"status"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

type Message struct {
	ID          string    `json:"id"`
	TransferID  string    `json:"transfer_id"`
	From        Side      `json:"from"`
	To          Side      `json:"to"`
	Kind        string    `json:"kind"`
	Secret      string    `json:"secret,omitempty"`
	Payload     string    `json:"payload,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	DeliveredAt time.Time `json:"delivered_at,omitempty"`
}

type ObservedState struct {
	Instance Instance
	Source   Transfer
	Dest     Transfer
}
