package socketio

import (
	"compress/flate"
	"compress/gzip"
	"embed"
	"io"
	"net/http"
	"path"
	"strings"
)

const EmbeddedClientVersion = "4.8.3"

//go:embed client-dist/*
var clientDist embed.FS

func (s *Server) wrapHTTPHandler(next http.Handler) http.Handler {
	if s == nil || !s.cfg.ServeClient {
		return next
	}
	prefix := strings.TrimRight(s.cfg.Path, "/") + "/"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, prefix) {
			filename := path.Base(r.URL.Path)
			if isClientAsset(filename) {
				s.serveClientAsset(w, r, filename)
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

func isClientAsset(filename string) bool {
	switch filename {
	case "socket.io.js",
		"socket.io.js.map",
		"socket.io.min.js",
		"socket.io.min.js.map",
		"socket.io.esm.min.js",
		"socket.io.esm.min.js.map",
		"socket.io.msgpack.min.js",
		"socket.io.msgpack.min.js.map":
		return true
	default:
		return false
	}
}

func (s *Server) serveClientAsset(w http.ResponseWriter, r *http.Request, filename string) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	data, err := clientDist.ReadFile("client-dist/" + filename)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	etag := `"` + EmbeddedClientVersion + `"`
	w.Header().Set("Cache-Control", "public, max-age=0")
	w.Header().Set("ETag", etag)
	if strings.HasSuffix(filename, ".map") {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
	} else {
		w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	}
	if match := r.Header.Get("If-None-Match"); match == etag || match == "W/"+etag {
		w.WriteHeader(http.StatusNotModified)
		return
	}
	if r.Method == http.MethodHead {
		w.Header().Set("Content-Length", integerString(len(data)))
		w.WriteHeader(http.StatusOK)
		return
	}

	encoding := preferredEncoding(r.Header.Get("Accept-Encoding"))
	switch encoding {
	case "gzip":
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Add("Vary", "Accept-Encoding")
		writer := gzip.NewWriter(w)
		w.WriteHeader(http.StatusOK)
		_, _ = writer.Write(data)
		_ = writer.Close()
	case "deflate":
		writer, createErr := flate.NewWriter(w, flate.DefaultCompression)
		if createErr != nil {
			http.Error(w, createErr.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Encoding", "deflate")
		w.Header().Add("Vary", "Accept-Encoding")
		w.WriteHeader(http.StatusOK)
		_, _ = writer.Write(data)
		_ = writer.Close()
	default:
		w.Header().Set("Content-Length", integerString(len(data)))
		w.WriteHeader(http.StatusOK)
		_, _ = io.Copy(w, strings.NewReader(string(data)))
	}
}

func preferredEncoding(value string) string {
	for _, candidate := range strings.Split(value, ",") {
		name := strings.TrimSpace(strings.SplitN(candidate, ";", 2)[0])
		if name == "gzip" || name == "deflate" {
			return name
		}
	}
	return ""
}

func integerString(value int) string {
	if value == 0 {
		return "0"
	}
	var buffer [32]byte
	index := len(buffer)
	for value > 0 {
		index--
		buffer[index] = byte('0' + value%10)
		value /= 10
	}
	return string(buffer[index:])
}
