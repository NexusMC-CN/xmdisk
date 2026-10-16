package webdav

import (
	"crypto/subtle"
	"net/http"
	"sync"

	xwebdav "golang.org/x/net/webdav"

	"xingmeng/internal/store"
)

// Router 返回 WebDAV 挂载路由：/dav/<bucket>/...（docs/04 数据面 HTTP 同端口）。
// Basic 认证 = 凭据表 AK/SK 且必须绑定该桶；带 Content-Length 的 PUT 超额度
// 直接 507（docs/04 口径；无长度的流式写入由 Write 即刻限流兜底，
// 经 x/net/webdav 固定映射为 405——已知偏离，记于 docs/06）。
func Router(st *store.Store) http.Handler {
	ls := xwebdav.NewMemLS() // 进程内锁（Office 类客户端需要）
	var mu sync.Mutex
	handlers := map[string]*xwebdav.Handler{}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rest := r.URL.Path[len("/dav/"):]
		bucket, _, _ := cutPath(rest)
		if bucket == "" {
			http.NotFound(w, r)
			return
		}
		user, pass, ok := r.BasicAuth()
		cred, err := st.LookupCred(r.Context(), user)
		if !ok || err != nil ||
			cred.Bucket != bucket ||
			subtle.ConstantTimeCompare([]byte(cred.Secret), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="xingmeng"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// 配额预检：声明长度超额度 → 507（不读 body，立即拒绝）
		if r.Method == http.MethodPut && r.ContentLength > 0 {
			if allowance, limited, err := st.QuotaAllowance(r.Context(), bucket, trimSlash(rest)); err == nil &&
				limited && r.ContentLength > allowance {
				http.Error(w, "quota exceeded", http.StatusInsufficientStorage)
				return
			}
		}
		mu.Lock()
		h, okH := handlers[bucket]
		if !okH {
			h = &xwebdav.Handler{
				Prefix:     "/dav/" + bucket,
				FileSystem: NewFS(st, bucket),
				LockSystem: ls,
			}
			handlers[bucket] = h
		}
		mu.Unlock()
		h.ServeHTTP(w, r)
	})
}

func cutPath(p string) (head, tail string, found bool) {
	for i := 0; i < len(p); i++ {
		if p[i] == '/' {
			return p[:i], p[i+1:], true
		}
	}
	return p, "", false
}

func trimSlash(p string) string {
	for len(p) > 0 && p[0] == '/' {
		p = p[1:]
	}
	return p
}
