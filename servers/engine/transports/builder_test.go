package transports

import (
	"slices"
	"testing"
)

func TestRegisteredTransportBuilders(t *testing.T) {
	registered := Transports()
	expected := map[string]struct {
		upgrades bool
		targets  []string
	}{
		POLLING:      {targets: []string{WEBSOCKET, WEBTRANSPORT}},
		WEBSOCKET:    {upgrades: true},
		WEBTRANSPORT: {upgrades: true},
	}
	for name, expectation := range expected {
		builder, exists := registered[name]
		if !exists {
			t.Fatalf("transport %q is not registered", name)
		}
		if builder.Name() != name || builder.HandlesUpgrades() != expectation.upgrades {
			t.Fatalf("unexpected %s builder metadata", name)
		}
		if !slices.Equal(builder.UpgradesTo(), expectation.targets) {
			t.Fatalf("%s upgrades = %v, want %v", name, builder.UpgradesTo(), expectation.targets)
		}
	}
}
