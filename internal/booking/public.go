package booking

import (
	"embed"
	"net/http"

	"github.com/mchupryna/soa-flight-booking/internal/api"
)

//go:embed home.html swagger/*
var publicFiles embed.FS

func home(w http.ResponseWriter, r *http.Request) {
	http.ServeFileFS(w, r, publicFiles, "home.html")
}

func specification(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodHead {
		_, _ = w.Write(api.OpenAPI)
	}
}
