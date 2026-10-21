// auth.go 是 admin 面的双 token 鉴权（99-D023⑤）：admin token（长期，
// env/YAML）与 session token（短期，POST /auth/session 签发、内存态）。
// 除签发/吊销全部两个端点要求 admin token 外，其余接口双轨接受。
package api

import (
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"sync"
	"time"
)

// SessionTTL 范围：1-24 小时（自选，正整数小时）。
const (
	SessionTTLMin = time.Hour
	SessionTTLMax = 24 * time.Hour
)

type sessionStore struct {
	mu sync.Mutex
	m  map[string]time.Time // token -> 过期时刻
}

func newSessionStore() *sessionStore {
	return &sessionStore{m: make(map[string]time.Time)}
}

func (s *sessionStore) issue(ttl time.Duration) (string, time.Time) {
	raw := make([]byte, 32)
	rand.Read(raw)
	tok := hex.EncodeToString(raw)
	exp := time.Now().Add(ttl)
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sweepLocked(time.Now())
	s.m[tok] = exp
	return tok, exp
}

func (s *sessionStore) valid(tok string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	exp, ok := s.m[tok]
	if !ok {
		return false
	}
	if time.Now().After(exp) {
		delete(s.m, tok)
		return false
	}
	return true
}

func (s *sessionStore) revoke(tok string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.m[tok]
	delete(s.m, tok)
	return ok
}

func (s *sessionStore) revokeAll() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := len(s.m)
	s.m = make(map[string]time.Time)
	return n
}

func (s *sessionStore) sweepLocked(now time.Time) {
	for k, exp := range s.m {
		if now.After(exp) {
			delete(s.m, k)
		}
	}
}

// tokenEqual 常数时间比较（长度差异本身可接受泄露）。
func tokenEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// auth 双轨中间件：X-Xm-Session 有效会话或 X-Xm-Token 匹配 admin token。
// 失败带固定延迟（S-15：与 adminAuth 一致，缓解在线爆破）。
func auth(adminToken string, sess *sessionStore, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if tok := r.Header.Get("X-Xm-Session"); tok != "" {
			if sess.valid(tok) {
				next.ServeHTTP(w, r)
				return
			}
			time.Sleep(250 * time.Millisecond)
			writeJSON(w, 401, map[string]any{"error": "session expired or invalid"})
			return
		}
		if tokenEqual(r.Header.Get("X-Xm-Token"), adminToken) {
			next.ServeHTTP(w, r)
			return
		}
		time.Sleep(250 * time.Millisecond)
		writeJSON(w, 401, map[string]any{"error": "bad or missing admin token"})
	})
}

// adminAuth 仅接受 admin token（签发与吊销全部；会话 token 不得续签/全清）。
// 失败带固定延迟，缓解爆破。
func adminAuth(adminToken string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if tokenEqual(r.Header.Get("X-Xm-Token"), adminToken) {
			next(w, r)
			return
		}
		time.Sleep(250 * time.Millisecond)
		writeJSON(w, 401, map[string]any{"error": "admin token required"})
	}
}

// handleIssueSession 用 admin token 换短时会话 token。
func handleIssueSession(sess *sessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req struct {
			TTLHours int `json:"ttl_hours"`
		}
		if err := readJSON(r, &req); err != nil {
			fail(w, err)
			return
		}
		if req.TTLHours < 1 || req.TTLHours > 24 {
			writeJSON(w, 400, map[string]any{"error": "ttl_hours must be an integer in [1, 24]"})
			return
		}
		ttl := time.Duration(req.TTLHours) * time.Hour
		tok, exp := sess.issue(ttl)
		writeJSON(w, 200, map[string]any{
			"token":       tok,
			"expires_at":  exp.Unix(),
			"expires_iso": exp.Format(time.RFC3339),
		})
	}
}

// handleLogout 会话自吊销（登出）。
func handleLogout(sess *sessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tok := r.Header.Get("X-Xm-Session")
		if tok == "" {
			writeJSON(w, 400, map[string]any{"error": "no session token supplied"})
			return
		}
		if !sess.revoke(tok) {
			writeJSON(w, 401, map[string]any{"error": "session expired or invalid"})
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

// handleRevokeAll 吊销全部会话（admin token；"踢掉所有浏览器"）。
func handleRevokeAll(sess *sessionStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		n := sess.revokeAll()
		writeJSON(w, 200, map[string]any{"revoked": n})
	}
}
