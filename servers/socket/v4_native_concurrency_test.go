package socket

import (
	"sync"
	"testing"
	"time"
)

func TestV4CoreConcurrentNamespaceCreationIsAtomic(t *testing.T) {
	server := NewServer(nil, nil)
	t.Cleanup(func() { server.Close(nil) })
	const callers = 64
	results := make(chan Namespace, callers)
	var wg sync.WaitGroup
	wg.Add(callers)
	for range callers {
		go func() { defer wg.Done(); results <- server.Of("/atomic", nil) }()
	}
	wg.Wait()
	close(results)
	var first Namespace
	for nsp := range results {
		if first == nil {
			first = nsp
			continue
		}
		if nsp != first {
			t.Fatalf("different namespace instances: %p != %p", nsp, first)
		}
	}
}

func TestV4CoreSocketTransientFlagsAreRaceSafe(t *testing.T) {
	socket := MakeSocket()
	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			socket.Volatile().Compress(i%2 == 0).Timeout(time.Duration(i+1) * time.Millisecond)
		}(i)
	}
	wg.Wait()
	_ = socket.takeFlags()
}
