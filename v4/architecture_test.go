package socketio

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestV4DoesNotDependOnLegacySocketServerRuntime(t *testing.T) {
	t.Parallel()

	legacyRuntime := "servers/" + "socket/v3"
	check := func(path string) {
		t.Helper()
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if strings.Contains(string(data), legacyRuntime) {
			t.Fatalf("%s reintroduces the legacy Socket.IO server runtime %q", path, legacyRuntime)
		}
	}

	check("go.mod")
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".go" {
			continue
		}
		check(entry.Name())
	}
}
