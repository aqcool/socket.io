package instrumentation

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"testing"
	"time"

	server "github.com/aqcool/socket.io/servers/socket/v3"
	"golang.org/x/crypto/bcrypt"
)

const adminUIOfficialInteropEnv = "SOCKET_IO_ADMIN_UI_OFFICIAL_INTEROP"

const adminUIOfficialGitHead = "232d87af04777b108725bd70c248b5e929a5057d"

type adminInteropEndpoints struct {
	Main       string `json:"main"`
	Custom     string `json:"custom"`
	Auth       string `json:"auth"`
	ReadOnly   string `json:"readOnly"`
	Production string `json:"production"`
}

func TestOfficialAdminUI051ProtocolInterop(t *testing.T) {
	if os.Getenv(adminUIOfficialInteropEnv) != "1" {
		t.Skip("set SOCKET_IO_ADMIN_UI_OFFICIAL_INTEROP=1 after npm ci in testdata/official-admin-ui")
	}

	mainURL := startAdminInteropServer(t, &Options{
		ServerID:      "official-admin-ui-node",
		StatsInterval: 25 * time.Millisecond,
	}, configureOfficialAdminMainServer)
	customURL := startAdminInteropServer(t, &Options{
		NamespaceName: "/custom",
		StatsInterval: 25 * time.Millisecond,
	}, nil)

	passwordHash, err := bcrypt.GenerateFromPassword([]byte("secret"), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	authURL := startAdminInteropServer(t, &Options{
		StatsInterval: 25 * time.Millisecond,
		BasicAuth: &BasicAuth{
			Username:     "admin",
			PasswordHash: string(passwordHash),
		},
	}, nil)
	readOnlyURL := startAdminInteropServer(t, &Options{
		ReadOnly:      true,
		StatsInterval: 25 * time.Millisecond,
	}, nil)
	productionURL := startAdminInteropServer(t, &Options{
		ReadOnly:      true,
		Mode:          ProductionMode,
		StatsInterval: 25 * time.Millisecond,
	}, nil)

	endpointJSON, err := json.Marshal(adminInteropEndpoints{
		Main:       mainURL,
		Custom:     customURL,
		Auth:       authURL,
		ReadOnly:   readOnlyURL,
		Production: productionURL,
	})
	if err != nil {
		t.Fatal(err)
	}

	commandContext, cancel := context.WithTimeout(t.Context(), 75*time.Second)
	defer cancel()
	command := exec.CommandContext(
		commandContext,
		"node",
		filepath.Join("testdata", "official-admin-ui", "matrix.cjs"),
	)
	command.Env = append(os.Environ(), "SOCKET_IO_ADMIN_UI_ENDPOINTS="+string(endpointJSON))
	output, err := command.CombinedOutput()
	if err != nil {
		if commandContext.Err() != nil {
			t.Fatalf("official Admin UI source matrix timed out: %v\n%s", commandContext.Err(), output)
		}
		t.Fatalf("official Admin UI source matrix failed: %v\n%s", err, output)
	}

	var result struct {
		OfficialClient  string `json:"officialClient"`
		OfficialPackage string `json:"officialPackage"`
		OfficialGitHead string `json:"officialGitHead"`
		VerifiedCases   []int  `json:"verifiedCases"`
	}
	if err = json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode official Admin UI result: %v\n%s", err, output)
	}
	wantCases := []int{1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 18, 19, 20, 21}
	if result.OfficialClient != "socket.io-client@4.8.3" ||
		result.OfficialPackage != "@socket.io/admin-ui@0.5.1" ||
		result.OfficialGitHead != adminUIOfficialGitHead ||
		!reflect.DeepEqual(result.VerifiedCases, wantCases) {
		t.Fatalf("incomplete official Admin UI source mapping: %+v", result)
	}
}

func startAdminInteropServer(t *testing.T, options *Options, configure func(*server.Server)) string {
	t.Helper()
	io := server.NewServer(nil, nil)
	instrumentation, err := Instrument(io, options)
	if err != nil {
		t.Fatal(err)
	}
	if configure != nil {
		configure(io)
	}
	httpServer := httptest.NewServer(io.ServeHandler(nil))
	t.Cleanup(func() {
		instrumentation.Close()
		io.Close(nil)
		httpServer.CloseClientConnections()
		httpServer.Close()
	})
	return httpServer.URL
}

func configureOfficialAdminMainServer(io *server.Server) {
	lifecycle := io.Of("/lifecycle", nil)
	_ = lifecycle.On("connection", func(values ...any) {
		socket := values[0].(*server.Socket)
		_ = socket.On("lifecycle-join", func(args ...any) {
			room := server.Room(args[0].(string))
			socket.Join(room)
			adminInteropAck(args)([]any{"joined"}, nil)
		})
		_ = socket.On("lifecycle-leave", func(args ...any) {
			room := server.Room(args[0].(string))
			socket.Leave(room)
			adminInteropAck(args)([]any{"left"}, nil)
		})
		_ = socket.On("lifecycle-disconnect", func(...any) {
			time.AfterFunc(10*time.Millisecond, func() { socket.Disconnect(false) })
		})
	})

	dataNamespace := io.Of("/data", nil)
	dataNamespace.Use(func(socket *server.Socket, next func(*server.ExtendedError)) {
		socket.SetData(map[string]any{"count": 1, "array": []any{1}})
		next(nil)
	})
	_ = dataNamespace.On("connection", func(values ...any) {
		socket := values[0].(*server.Socket)
		_ = socket.On("set-data", func(args ...any) {
			socket.SetData(args[0])
			adminInteropAck(args)([]any{"updated"}, nil)
		})
	})

	management := io.Of("/management", nil)
	_ = management.On("connection", func(values ...any) {
		socket := values[0].(*server.Socket)
		_ = socket.On("has-room", func(args ...any) {
			room := server.Room(args[0].(string))
			adminInteropAck(args)([]any{socket.Rooms().Has(room)}, nil)
		})
	})

	events := io.Of("/events", nil)
	_ = events.On("connection", func(values ...any) {
		socket := values[0].(*server.Socket)
		_ = socket.On("received-no-ack", func(...any) {})
		_ = socket.On("received-with-ack", func(args ...any) {
			adminInteropAck(args)([]any{"123"}, nil)
		})
		_ = socket.On("trigger-sent-no-ack", func(args ...any) {
			_ = socket.Emit("sent-no-ack", 1, "2", []byte{3})
			adminInteropAck(args)([]any{"sent"}, nil)
		})
		_ = socket.On("trigger-sent-with-ack", func(args ...any) {
			controlAck := adminInteropAck(args)
			socket.EmitWithAck("sent-with-ack")(func(values []any, err error) {
				if err != nil {
					controlAck(nil, err)
					return
				}
				if len(values) == 0 {
					controlAck(nil, nil)
					return
				}
				controlAck([]any{values[0]}, nil)
			})
		})
	})

	io.Of(regexp.MustCompile(`^/dynamic-\d+$`), nil)
}

func adminInteropAck(args []any) server.Ack {
	if len(args) == 0 {
		return func([]any, error) {}
	}
	ack, ok := args[len(args)-1].(server.Ack)
	if !ok {
		return func([]any, error) {}
	}
	return ack
}
