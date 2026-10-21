package repl

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"xingmeng/internal/errs"
	"xingmeng/internal/meta"
	"xingmeng/internal/store"
)

// ErrNeedResnapshot 表示从节点落后超出主节点日志保留期，须转全量重建。
var ErrNeedResnapshot = errors.New("slave beyond master log retention, bootstrap required")

type SlaveConfig struct {
	ID        string
	MasterURL string // 例如 http://10.0.0.2:9003
	Token     string
	Batch     int           // 单批 op 数（默认 500）
	IdleWait  time.Duration // 追平后的轮询间隔（默认 1s）
}

// Slave 是从节点同步引擎：拉 op → 补齐 blob（墓碑感知）→ 事务回放 → ack。
// 回放语义见 store.ApplyOp（幂等、原 seq 入日志、epoch fencing）。
type Slave struct {
	cfg        SlaveConfig
	store      *store.Store
	hcCtrl     *http.Client    // 控制请求（oplog/register/ack）：30s 超时
	hcData     *http.Client    // 数据传输（blob/快照）：15min 超时，超时重试续传
	tombstones map[string]bool // 主端已墓碑化的 blob（410 学得，每轮清空防复活失联）
	lastSeq    int64
	maxEpoch   uint32
}

func NewSlave(cfg SlaveConfig, s *store.Store) (*Slave, error) {
	if cfg.ID == "" || cfg.MasterURL == "" {
		return nil, fmt.Errorf("%w: slave ID and MasterURL are required", errs.ErrInvalidArgument)
	}
	if cfg.Batch <= 0 {
		cfg.Batch = 500
	}
	if cfg.IdleWait <= 0 {
		cfg.IdleWait = time.Second
	}
	ctx := context.Background()
	last, err := s.LastSeq(ctx)
	if err != nil {
		return nil, err
	}
	epoch, err := s.MaxEpoch(ctx)
	if err != nil {
		return nil, err
	}
	return &Slave{
		cfg: cfg, store: s,
		hcCtrl:     &http.Client{Timeout: 30 * time.Second},
		hcData:     &http.Client{Timeout: 15 * time.Minute},
		tombstones: map[string]bool{},
		lastSeq:    last,
		maxEpoch:   epoch,
	}, nil
}

// Register 向主节点登记并做 epoch 握手：主节点 epoch 低于本地已见最大值
// 说明对面是旧主复活，拒绝同步（99-D005 fencing 的从端侧）。
func (sl *Slave) Register(ctx context.Context) error {
	var resp registerResp
	if err := sl.postJSON(ctx, "/register", registerReq{SlaveID: sl.cfg.ID}, &resp); err != nil {
		return err
	}
	if resp.Epoch < sl.maxEpoch {
		return fmt.Errorf("%w: master epoch %d < seen %d", errs.ErrStaleEpoch, resp.Epoch, sl.maxEpoch)
	}
	return nil
}

// SyncOnce 拉一批 op 并回放，返回本轮应用的 op 数。
func (sl *Slave) SyncOnce(ctx context.Context) (int, error) {
	sl.tombstones = map[string]bool{}
	resp, err := sl.get(ctx, fmt.Sprintf("/oplog?after=%d&limit=%d", sl.lastSeq, sl.cfg.Batch))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		io.Copy(io.Discard, resp.Body)
		return 0, ErrNeedResnapshot
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("oplog: status %d: %s", resp.StatusCode, snippet(resp.Body))
	}
	var body oplogResp
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, err
	}
	for _, w := range body.Ops {
		op := fromWire(w)
		if err := sl.ensureBlobs(ctx, op); err != nil {
			return 0, fmt.Errorf("ensure blobs for op %d: %w", op.Seq, err)
		}
		if err := sl.store.ApplyOp(ctx, op); err != nil {
			return 0, fmt.Errorf("apply op %d (%s): %w", op.Seq, op.Type, err)
		}
		sl.lastSeq = op.Seq
		if op.Epoch > sl.maxEpoch {
			sl.maxEpoch = op.Epoch
		}
	}
	if len(body.Ops) > 0 {
		if err := sl.ack(ctx); err != nil {
			return 0, err
		}
	}
	return len(body.Ops), nil
}

// Bootstrap 全量重建：下载快照替换本地元数据（blob 保留，内容寻址天然可续），
// 随后从快照截断点继续 tail。
func (sl *Slave) Bootstrap(ctx context.Context) error {
	resp, err := sl.get(ctx, "/snapshot")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("snapshot: status %d: %s", resp.StatusCode, snippet(resp.Body))
	}
	tmp, err := os.CreateTemp(sl.store.DataDir(), "snap-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) // ReplaceMeta 成功后已改名，此处清理失败残留
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	seq, err := strconv.ParseInt(resp.Header.Get("X-Xm-Seq"), 10, 64)
	if err != nil {
		return fmt.Errorf("snapshot seq header: %w", err)
	}
	if err := sl.store.ReplaceMeta(tmpName); err != nil {
		return err
	}
	sl.lastSeq = seq
	if epoch, err := sl.store.MaxEpoch(ctx); err == nil {
		sl.maxEpoch = epoch
	}
	// 快照只带元数据：按本地条目清单补齐缺失 blob（内容寻址，已有 blob 自动跳过）
	if err := sl.syncAllBlobs(ctx); err != nil {
		return fmt.Errorf("fetch blobs after bootstrap: %w", err)
	}
	slog.Info("slave bootstrapped from snapshot", "seq", seq)
	return sl.ack(ctx)
}

// syncAllBlobs 遍历本地元数据引用的全部 blob，缺失的从主节点拉取（墓碑感知同 ensureBlobs）。
func (sl *Slave) syncAllBlobs(ctx context.Context) error {
	sl.tombstones = map[string]bool{}
	return sl.store.EachBlobRef(ctx, func(sha string) error {
		if sl.store.HasBlob(sha) || sl.tombstones[sha] {
			return nil
		}
		return sl.fetchBlob(ctx, sha)
	})
}

// Run 持续同步：落后超限自动转 Bootstrap；错误 1s 退避；ctx 取消即停。
func (sl *Slave) Run(ctx context.Context) {
	if err := sl.Register(ctx); err != nil {
		slog.Error("slave register", "err", err) // 不中断：oplog 拉取本身可自恢复
	}
	for {
		n, err := sl.SyncOnce(ctx)
		switch {
		case err == nil:
			if n == 0 {
				// 空转心跳：刷新主端 last_seen，防活跃窗口（99-D026）误判已追平的
				// 从节点为离线、进而被截断日志逼着无谓重建。
				if aerr := sl.ack(ctx); aerr != nil {
					slog.Debug("slave idle ack", "err", aerr)
				}
				if !sleepCtx(ctx, sl.cfg.IdleWait) {
					return
				}
			}
		case errors.Is(err, ErrNeedResnapshot):
			slog.Warn("slave beyond retention, bootstrapping")
			if berr := sl.Bootstrap(ctx); berr != nil {
				slog.Error("slave bootstrap", "err", berr)
				if !sleepCtx(ctx, time.Second) {
					return
				}
			}
		default:
			slog.Error("slave sync", "err", err)
			if !sleepCtx(ctx, time.Second) {
				return
			}
		}
	}
}

func (sl *Slave) ack(ctx context.Context) error {
	return sl.postJSON(ctx, "/ack", ackReq{SlaveID: sl.cfg.ID, Epoch: sl.maxEpoch, LastSeq: sl.lastSeq}, nil)
}

// ensureBlobs 在回放 entry_put 前补齐其引用的 blob。主端返回 410 墓碑时
// 记录"已知已删"并跳过——日志顺序保证引用它的条目随后会被删除，最终态一致。
func (sl *Slave) ensureBlobs(ctx context.Context, op meta.Op) error {
	if op.Type != meta.OpEntryPut {
		return nil
	}
	var p struct {
		Blobs []struct {
			Sha256 string `json:"sha256"`
		} `json:"blobs"`
	}
	if err := json.Unmarshal([]byte(op.Payload), &p); err != nil {
		return fmt.Errorf("%w: op %d payload: %v", errs.ErrInternal, op.Seq, err)
	}
	for _, ref := range p.Blobs {
		if sl.store.HasBlob(ref.Sha256) || sl.tombstones[ref.Sha256] {
			continue
		}
		if err := sl.fetchBlob(ctx, ref.Sha256); err != nil {
			return err
		}
	}
	return nil
}

// fetchBlob 流式拉取 blob：内容直接灌入本地数据面，边写边算哈希，
// 与请求哈希比对即完整性校验（内容寻址的免费午餐）。
func (sl *Slave) fetchBlob(ctx context.Context, sha string) error {
	resp, err := sl.get(ctx, "/blob/"+sha)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch {
	case resp.StatusCode == http.StatusGone:
		io.Copy(io.Discard, resp.Body)
		sl.tombstones[sha] = true
		return nil
	case resp.StatusCode != http.StatusOK:
		return fmt.Errorf("blob %s: status %d: %s", sha, resp.StatusCode, snippet(resp.Body))
	}
	h, _, err := sl.store.WriteBlob(resp.Body)
	if err != nil {
		return err
	}
	if h != sha {
		return fmt.Errorf("%w: blob %s hash mismatch (got %s)", errs.ErrInternal, sha, h)
	}
	return nil
}

func (sl *Slave) get(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sl.cfg.MasterURL+path, nil)
	if err != nil {
		return nil, err
	}
	if sl.cfg.Token != "" {
		req.Header.Set("X-Xm-Token", sl.cfg.Token)
	}
	if strings.HasPrefix(path, "/blob/") || path == "/snapshot" {
		return sl.hcData.Do(req)
	}
	return sl.hcCtrl.Do(req)
}

func (sl *Slave) postJSON(ctx context.Context, path string, body, out any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sl.cfg.MasterURL+path, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if sl.cfg.Token != "" {
		req.Header.Set("X-Xm-Token", sl.cfg.Token)
	}
	resp, err := sl.hcCtrl.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s: status %d: %s", path, resp.StatusCode, snippet(resp.Body))
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	return nil
}

func snippet(r io.Reader) string {
	b, _ := io.ReadAll(io.LimitReader(r, 512))
	return string(b)
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
