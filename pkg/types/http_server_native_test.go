package types

import (
	"errors"
	"net"
	"testing"
	"time"
)

func TestListenAndServeReturnsBindError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	server := NewWebServer(nil)
	if err := server.ListenAndServe(listener.Addr().String()); err == nil {
		t.Fatal("expected bind error")
	}
}

func TestServeReturnsAfterClose(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := NewWebServer(nil)
	serveDone := make(chan error, 1)
	go func() {
		serveDone <- server.Serve(listener)
	}()

	deadline := time.Now().Add(time.Second)
	for server.servers.Len() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.servers.Len() == 0 {
		t.Fatal("server was not registered")
	}

	if err := server.Close(nil); err != nil && !errors.Is(err, ErrServerNotRunning) {
		t.Fatalf("close: %v", err)
	}

	select {
	case err := <-serveDone:
		if err != nil {
			t.Fatalf("serve returned error after close: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Serve did not return after Close")
	}
}
