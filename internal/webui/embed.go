package webui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed assets/*
var files embed.FS

func Handler() http.Handler {
	content, err := fs.Sub(files, "assets")
	if err != nil {
		panic(err)
	}
	return http.FileServer(http.FS(content))
}
