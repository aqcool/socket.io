package typed

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOfficialTypeContractPositiveFixtureCompiles(t *testing.T) {
	command := exec.Command("go", "test", "./testdata/compile/pass")
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("positive type fixture must compile: %v\n%s", err, output)
	}
}

func TestOfficialTypeContractRejectsInvalidCalls(t *testing.T) {
	command := exec.Command("go", "test", "./testdata/compile/fail")
	output, err := command.CombinedOutput()
	if err == nil {
		t.Fatalf("negative type fixture unexpectedly compiled:\n%s", output)
	}
	for _, marker := range []string{
		"wrong request type",
		"func(context.Context, string)",
		"as string value in assignment",
		"typed.ServerSideEmit",
		"not an adapter",
	} {
		if !strings.Contains(string(output), marker) {
			t.Fatalf("compiler output does not prove rejection of %q:\n%s", marker, output)
		}
	}
}

func TestGeneratedClientOfficialTypeContract(t *testing.T) {
	files, err := Generate([]Namespace{NewNamespace("/",
		Event[string, NoAck]{Name: "outgoing", Direction: ClientToServer}.Definition(),
		Event[string, NoAck]{Name: "incoming", Direction: ServerToClient}.Definition(),
		Event[string, bool]{Name: "request-ack", Direction: ClientToServer}.Definition(),
	)}, GenerateOptions{Package: "contract"})
	if err != nil {
		t.Fatal(err)
	}
	typedRoot, err := filepath.Abs(".")
	if err != nil {
		t.Fatal(err)
	}

	endpointSource := `package contract

import "github.com/aqcool/socket.io/v4/pkg/types"

type endpoint struct{}
func (*endpoint) Emit(string, ...any) error { return nil }
func (*endpoint) On(string, ...types.EventListener) error { return nil }
`
	positiveSource := `package contract

import (
	"context"
	sockettyped "github.com/aqcool/socket.io/typed/v4"
)

func positiveClientContract(value *endpoint) {
	_ = EmitOutgoing(value, "payload")
	_ = OnIncoming(value, func(context.Context, string) error { return nil })
	_ = sockettyped.OnConnect(value, func(context.Context) error { return nil })
	_ = sockettyped.OnConnectError(value, func(context.Context, error) error { return nil })
	_ = sockettyped.OnDisconnect(value, func(context.Context, sockettyped.DisconnectReason) error { return nil })
	var acknowledged bool
	acknowledged, _ = EmitRequestAck(context.Background(), value, "payload")
	_ = acknowledged
}
`
	negativeSource := `package contract

import "context"

func negativeClientContract(value *endpoint) {
	_ = EmitOutgoing(value, 42)
	_ = OnIncoming(value, func(context.Context, int) error { return nil })
	_ = OnOutgoing(value, func(context.Context, string) error { return nil })
	_ = EmitIncoming(value, "wrong direction")
	var wrong string
	wrong, _ = EmitRequestAck(context.Background(), value, "payload")
	_ = wrong
}
`

	writeFixture := func(name, contractSource string) string {
		t.Helper()
		directory := t.TempDir()
		repositoryRoot := filepath.Dir(typedRoot)
		module := fmt.Sprintf(`module generated-%s

go 1.26.0

require github.com/aqcool/socket.io/typed/v4 v3.0.4

replace (
	github.com/aqcool/socket.io/typed/v4 => %s
	github.com/aqcool/socket.io/servers/socket/v4 => %s
	github.com/aqcool/socket.io/servers/engine/v4 => %s
	github.com/aqcool/socket.io/parsers/socket/v4 => %s
	github.com/aqcool/socket.io/parsers/engine/v4 => %s
	github.com/aqcool/socket.io/v4 => %s
)
`, name, typedRoot,
			filepath.Join(repositoryRoot, "servers", "socket"),
			filepath.Join(repositoryRoot, "servers", "engine"),
			filepath.Join(repositoryRoot, "parsers", "socket"),
			filepath.Join(repositoryRoot, "parsers", "engine"),
			repositoryRoot,
		)
		for filename, payload := range map[string][]byte{
			"go.mod":                 []byte(module),
			"socketio_client.gen.go": files["socketio_client.gen.go"],
			"endpoint.go":            []byte(endpointSource),
			"contract.go":            []byte(contractSource),
		} {
			if writeErr := os.WriteFile(filepath.Join(directory, filename), payload, 0o600); writeErr != nil {
				t.Fatal(writeErr)
			}
		}
		return directory
	}

	positive := exec.Command("go", "test", "-mod=mod", "./...")
	positive.Dir = writeFixture("positive", positiveSource)
	if output, compileErr := positive.CombinedOutput(); compileErr != nil {
		t.Fatalf("generated positive client contract must compile: %v\n%s", compileErr, output)
	}

	negative := exec.Command("go", "test", "-mod=mod", "./...")
	negative.Dir = writeFixture("negative", negativeSource)
	output, err := negative.CombinedOutput()
	if err == nil {
		t.Fatalf("generated negative client contract unexpectedly compiled:\n%s", output)
	}
	for _, marker := range []string{
		"cannot use 42",
		"func(context.Context, int)",
		"undefined: OnOutgoing",
		"undefined: EmitIncoming",
		"as string value in assignment",
	} {
		if !strings.Contains(string(output), marker) {
			t.Fatalf("generated compiler output does not prove rejection of %q:\n%s", marker, output)
		}
	}
}
