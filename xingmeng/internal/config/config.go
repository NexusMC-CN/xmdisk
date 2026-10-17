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
		MasterAddr string `yaml:"master_addr"` // slave 用（初值；运行态 SyncAddr 优先，99-D023）
		Token      string `yaml:"token"`       // repl 面共享凭证（初值；为空 = 不监听复制面）
	}
	AdminToken     string `yaml:"admin_token"`     // admin 面凭证（必填，XM_ADMIN_TOKEN 优先）
	PublicEndpoint string `yaml:"public_endpoint"` // 对外展示地址（WebUI 连接信息用；空 = 按请求推断）
	WebUI          *bool  `yaml:"webui"`           // 内嵌管理页开关（默认开）
	Log            struct {
		Level string `yaml:"level"`
	}
}

// WebUIEnabled 报告管理页是否启用（缺省 true，99-D023）。
func (c *Config) WebUIEnabled() bool { return c.WebUI == nil || *c.WebUI }

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

// Defaults 返回全默认配置（无配置文件形态，99-D023：docker 拉起即用）。
func Defaults() *Config {
	c := &Config{}
	c.applyDefaults()
	if err := c.validate(); err != nil {
		panic("default config must be valid: " + err.Error())
	}
	return c
}

func (c *Config) applyDefaults() {
	if c.Node.Role == "" {
		c.Node.Role = "master"
	}
	if c.Node.DataDir == "" {
		c.Node.DataDir = "./var"
	}
	if c.Listeners.DataHTTP == "" {
		c.Listeners.DataHTTP = "0.0.0.0:9000"
	}
	if c.Listeners.Admin == "" {
		// 0.0.0.0：docker 形态默认可用（admin token 必填即安全，99-D023②）；
		// 裸机想只留本机访问可显式配 127.0.0.1。
		c.Listeners.Admin = "0.0.0.0:9002"
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
	// slave 无 master_addr 允许启动（99-D023：空转安全，serve 恒 off，
	// 地址可后经 admin API 设置并持久化于运行态）。
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

// ApplyEnv 用 XM_* 环境变量覆盖配置初值（99-D023 优先级：运行态 > env > YAML > 默认）。
// env 是 docker 形态的出生配置面（MySQL root 密码模式）；返回被覆盖的键便于启动日志展示。
func ApplyEnv(c *Config) ([]string, error) {
	applied := []string{}
	if v := os.Getenv("XM_DATA_DIR"); v != "" {
		c.Node.DataDir = v
		applied = append(applied, "XM_DATA_DIR")
	}
	if v := os.Getenv("XM_ROLE"); v != "" {
		if v != "master" && v != "slave" {
			return nil, fmt.Errorf("XM_ROLE must be master or slave, got %q", v)
		}
		c.Node.Role = v
		applied = append(applied, "XM_ROLE")
	}
	if v := os.Getenv("XM_ADMIN_TOKEN"); v != "" {
		c.AdminToken = v
		applied = append(applied, "XM_ADMIN_TOKEN")
	}
	if v := os.Getenv("XM_REPL_TOKEN"); v != "" {
		c.Replication.Token = v
		applied = append(applied, "XM_REPL_TOKEN")
	}
	if v := os.Getenv("XM_MASTER_ADDR"); v != "" {
		c.Replication.MasterAddr = v
		applied = append(applied, "XM_MASTER_ADDR")
	}
	if v := os.Getenv("XM_PUBLIC_ENDPOINT"); v != "" {
		c.PublicEndpoint = v
		applied = append(applied, "XM_PUBLIC_ENDPOINT")
	}
	if v := os.Getenv("XM_WEBUI"); v != "" {
		switch strings.ToLower(v) {
		case "on", "1", "true":
			t := true
			c.WebUI = &t
		case "off", "0", "false":
			f := false
			c.WebUI = &f
		default:
			return nil, fmt.Errorf("XM_WEBUI must be on|off, got %q", v)
		}
		applied = append(applied, "XM_WEBUI")
	}
	return applied, nil
}
