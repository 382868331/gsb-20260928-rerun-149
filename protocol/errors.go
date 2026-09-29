package protocol

import "errors"

var (
	ErrNotFound         = errors.New("not found")
	ErrAlreadyExists    = errors.New("already exists")
	ErrInsufficient     = errors.New("insufficient available balance")
	ErrInvalidState     = errors.New("invalid transfer state")
	ErrTimeout          = errors.New("lock has not expired")
	ErrLockExpired      = errors.New("lock has expired")
	ErrBadSecret        = errors.New("secret does not match hash lock")
	ErrDuplicateMessage = errors.New("duplicate message")
	ErrUnavailable      = errors.New("ledger unavailable")
	ErrConflict         = errors.New("outcome conflict")
)
