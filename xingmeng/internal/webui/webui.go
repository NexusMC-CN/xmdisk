// Package webui 是内嵌的极简管理页（99-D023⑥）：go:embed 单文件、无构建链、
// 无外部依赖；从 admin 端口同源下发，浏览器以 X-Xm-Session 会话 token 直调
// admin API（同源免 CORS，不用 cookie 即无 CSRF 面）。
package webui

import (
	"embed"
	"net/http"
)

//go:embed index.html
var files embed.FS

// Handler 下发管理页；任意 GET 均回 index.html（单页应用，无深链）。
func Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Frame-Options", "DENY")
		// CSP 兜底：默认仅同源，脚本/样式因单文件内联放开（全部插值经 esc() 转义）
		w.Header().Set("Content-Security-Policy",
			"default-src 'self'; script-src 'unsafe-inline'; style-src 'unsafe-inline'; connect-src 'self'")
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		data, err := files.ReadFile("index.html")
		if err != nil {
			http.Error(w, "webui embedded asset missing", http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(data)
	})
}
