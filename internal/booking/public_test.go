package booking

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/mchupryna/soa-flight-booking/internal/api"
)

func TestPublicPages(t *testing.T) {
	h := New(nil, nil, nil, DefaultConfig()).Handler()
	for _, tc := range []struct{ path, contentType string }{
		{"/", "text/html"},
		{"/openapi.json", "application/json"},
		{"/swagger/", "text/html"},
		{"/swagger/swagger-ui.css", "text/css"},
		{"/swagger/swagger-ui-bundle.js", "text/javascript"},
	} {
		for _, method := range []string{http.MethodGet, http.MethodHead} {
			t.Run(method+tc.path, func(t *testing.T) {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(method, tc.path, nil))
				if w.Code != http.StatusOK || !strings.HasPrefix(w.Header().Get("Content-Type"), tc.contentType) {
					t.Fatalf("status=%d headers=%v", w.Code, w.Header())
				}
				if method == http.MethodHead {
					if w.Body.Len() != 0 {
						t.Fatal("HEAD response contains a body")
					}
					return
				}
				if w.Body.Len() == 0 {
					t.Fatal("empty public page or asset")
				}
				if tc.path == "/" {
					for _, link := range []string{`href="/swagger/"`, `href="/health"`, `href="/openapi.json"`} {
						if !strings.Contains(w.Body.String(), link) {
							t.Fatalf("missing navigation link %s", link)
						}
					}
				}
				if tc.path == "/openapi.json" && !bytes.Equal(w.Body.Bytes(), api.OpenAPI) {
					t.Fatal("response differs from the API specification")
				}
			})
		}
	}
}

func TestPublicRouting(t *testing.T) {
	h := New(nil, nil, nil, DefaultConfig()).Handler()
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/swagger", nil))
	if w.Code != http.StatusPermanentRedirect || w.Header().Get("Location") != "/swagger/" {
		t.Fatalf("redirect: status=%d headers=%v", w.Code, w.Header())
	}
	for _, tc := range []struct {
		method, path string
		status       int
	}{
		{"GET", "/missing", 404},
		{"GET", "/swagger/missing.js", 404},
		{"GET", "/swagger/LICENSE", 404},
		{"POST", "/", 405},
		{"POST", "/swagger/", 405},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
			if w.Code != tc.status {
				t.Fatalf("status=%d, want %d", w.Code, tc.status)
			}
		})
	}
}
