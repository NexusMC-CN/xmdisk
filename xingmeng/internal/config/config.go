// Package config 加载节点配置（YAML，docs/05）与运行态（node-state.json，99-D016）。
// KEK 不进配置文件：仅从环境变量 XM_KEK 读取（开发期 .env，99-D009）。
package config

import (
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// ByteSize 支持 "4MB"/"512KB"/"1TB" 或纯字节数（二进制语义，KB=1024）。
type ByteSize int64

var sizeRe = regexp.MustCompile(`^(?i)([0-9]+(?:\.[0-9]+)?)\s*(b|kb|mb|gb|tb|kib|mib|gib|tib)$`)

func (b *ByteSize) UnmarshalYAML(value *yaml.Node) error {
	s := strings.TrimSpace(value.Value)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		*b = ByteSize(n)
		return nil
	}
	m := sizeRe.FindStringSubmatch(s)
	if m == nil {
		return fmt.Errorf("invalid size %q (expect 4194304 / 4MB / 512KB ...)", s)
	}
	num, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return err
	}
	unit := map[string]float64{
		"b": 1, "kb": 1 << 10, "mb": 1 << 20, "gb": 1 << 30, "tb": 1 << 40,
	}[strings.ToLower(m[2])]
	*b = ByteSize(num * unit)
	return nil
}

type Config struct {
	Node struct {
		Role    string `yaml:"role"`     // master | slave（初值；运行态优先）
		ID      string `yaml:"id"`       // 从节点标识，缺省 hostname
		DataDir string `yaml:"data_dir"` // 必填
	}
	Listeners struct {
		DataHTTP string `yaml:"data_http"`
		DataSSH  string `yaml:"data_ssh"` // SFTP 适配未实现，暂不监听
		Admin    string `yaml:"admin"`
		Repl     string `yaml:"repl"`
	}
	Storage struct {
		BlobSmallThreshold  ByteSize      `yaml:"blob_small_threshold"`
		ChunkSize           ByteSize      `yaml:"chunk_size"`
		MaxObjectSize       ByteSize      `yaml:"max_object_size"`
		MultipartMaxSize    ByteSize      `yaml:"multipart_max_object"`
		GCInterval          time.Duration `yaml:"gc_interval"`
		GCGrace             time.Duration `yaml:"gc_grace"`
		MultipartAbortAfter time.Duration `yaml:"multipart_abort_after"`
	}
	Replication struct {
		MasterAddr string `yaml:"master_addr"` // slave 用
		Token      string `yaml:"token"`
	}
	AdminToken string `yaml:"admin_token"` // admin 面凭证；空 = 不鉴权（开发绑 127.0.0.1）
	Log        struct {
		Level string `yaml:"level"`
	}
}

// Load 读取并校验配置，填充默认值（与 docs/05 一致）。
func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &Config{}
	if err := yaml.Unmarshal(raw, c); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Config) applyDefaults() {
	if c.Node.Role == "" {
		c.Node.Role = "master"
	}
	if c.Listeners.DataHTTP == "" {
		c.Listeners.DataHTTP = "0.0.0.0:9000"
	}
	if c.Listeners.Admin == "" {
		c.Listeners.Admin = "127.0.0.1:9002"
	}
	if c.Listeners.Repl == "" {
		c.Listeners.Repl = "0.0.0.0:9003"
	}
	if c.Storage.BlobSmallThreshold == 0 {
		c.Storage.BlobSmallThreshold = 4 << 20
	}
	if c.Storage.ChunkSize == 0 {
		c.Storage.ChunkSize = 8 << 20
	}
	if c.Storage.MaxObjectSize == 0 {
		c.Storage.MaxObjectSize = 5 << 30
	}
	if c.Storage.MultipartMaxSize == 0 {
		c.Storage.MultipartMaxSize = 1 << 40
	}
	if c.Storage.GCInterval == 0 {
		c.Storage.GCInterval = time.Hour
	}
	if c.Storage.GCGrace == 0 {
		c.Storage.GCGrace = 48 * time.Hour
	}
	if c.Storage.MultipartAbortAfter == 0 {
		c.Storage.MultipartAbortAfter = 168 * time.Hour
	}
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
}

func (c *Config) validate() error {
	if c.Node.Role != "master" && c.Node.Role != "slave" {
		return fmt.Errorf("node.role must be master or slave, got %q", c.Node.Role)
	}
	if c.Node.DataDir == "" {
		return fmt.Errorf("node.data_dir is required")
	}
	if c.Node.Role == "slave" && c.Replication.MasterAddr == "" {
		return fmt.Errorf("slave requires replication.master_addr")
	}
	return nil
}

// NodeID 返回从节点标识（缺省 hostname）。
func (c *Config) NodeID() string {
	if c.Node.ID != "" {
		return c.Node.ID
	}
	h, err := os.Hostname()
	if err != nil || h == "" {
		return "node-1"
	}
	return h
}
