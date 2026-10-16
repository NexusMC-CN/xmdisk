package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// State 是运行态（node-state.json，99-D016）：配置文件只声明初值，
// promote / serve / sync 等运行期变更持久化于此，重启时优先于配置。
// 灾难重建（清元数据）时连带清空本文件，节点回到配置初值。
type State struct {
	Role      string `json:"role"`  // master | slave
	Epoch     uint32 `json:"epoch"` // op 生成 epoch
	Serve     string `json:"serve"` // off | ro | rw
	UpdatedAt int64  `json:"updated_at"`
}

func statePath(dataDir string) string {
	return filepath.Join(dataDir, "node-state.json")
}

// LoadState 读取运行态；文件不存在返回 nil（用配置初值）。
func LoadState(dataDir string) (*State, error) {
	raw, err := os.ReadFile(statePath(dataDir))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st State
	if err := json.Unmarshal(raw, &st); err != nil {
		return nil, fmt.Errorf("parse node-state.json: %w", err)
	}
	return &st, nil
}

// SaveState 原子写运行态（临时文件 + rename）。
func SaveState(dataDir string, st *State) error {
	raw, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}
	p := statePath(dataDir)
	tmp := p + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, p)
}
