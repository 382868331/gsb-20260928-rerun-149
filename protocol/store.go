package protocol

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
)

type JSONStore struct {
	AccountsPath  string
	TransfersPath string
}

func NewJSONStore(dir string) JSONStore {
	return JSONStore{
		AccountsPath:  filepath.Join(dir, "accounts.json"),
		TransfersPath: filepath.Join(dir, "transfers.json"),
	}
}

func loadJSON[T any](path string, makeValue func() T) (T, error) {
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return makeValue(), nil
	}
	if err != nil {
		var zero T
		return zero, err
	}
	value := makeValue()
	if len(data) == 0 {
		return value, nil
	}
	if err := json.Unmarshal(data, &value); err != nil {
		var zero T
		return zero, err
	}
	return value, nil
}

func saveJSON[T any](path string, value T) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func (s JSONStore) LoadAccounts(context.Context) (map[string]Account, error) {
	return loadJSON(s.AccountsPath, func() map[string]Account { return map[string]Account{} })
}

func (s JSONStore) SaveAccounts(_ context.Context, accounts map[string]Account) error {
	return saveJSON(s.AccountsPath, accounts)
}

func (s JSONStore) LoadTransfers(context.Context) (map[string]Transfer, error) {
	return loadJSON(s.TransfersPath, func() map[string]Transfer { return map[string]Transfer{} })
}

func (s JSONStore) SaveTransfers(_ context.Context, transfers map[string]Transfer) error {
	return saveJSON(s.TransfersPath, transfers)
}
