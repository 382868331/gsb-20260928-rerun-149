package protocol

import (
	"context"
	"os"
	"path/filepath"
)

type ProtocolStore struct {
	InstancesPath string
	MessagesPath  string
}

func NewProtocolStore(dir string) ProtocolStore {
	return ProtocolStore{
		InstancesPath: filepath.Join(dir, "instances.json"),
		MessagesPath:  filepath.Join(dir, "messages.json"),
	}
}

func (s ProtocolStore) LoadInstances(context.Context) (map[string]Instance, error) {
	return loadJSON(s.InstancesPath, func() map[string]Instance { return map[string]Instance{} })
}

func (s ProtocolStore) SaveInstances(_ context.Context, instances map[string]Instance) error {
	return saveJSON(s.InstancesPath, instances)
}

func (s ProtocolStore) LoadMessages(context.Context) (map[string]Message, error) {
	return loadJSON(s.MessagesPath, func() map[string]Message { return map[string]Message{} })
}

func (s ProtocolStore) SaveMessages(_ context.Context, messages map[string]Message) error {
	if err := os.MkdirAll(filepath.Dir(s.MessagesPath), 0o755); err != nil {
		return err
	}
	return saveJSON(s.MessagesPath, messages)
}
