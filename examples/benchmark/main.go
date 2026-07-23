package main

import (
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	io_client "github.com/aqcool/socket.io/clients/socket/v3"
	io "github.com/aqcool/socket.io/servers/socket/v3"
)

func main() {
	// Enable PPROF
	go func() {
		log.Println("Starting PPROF on :6060. View memory with `go tool pprof http://localhost:6060/debug/pprof/heap`")
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	// Setup Server
	srv := io.NewServer(nil, nil)
	srv.On("connection", func(clients ...any) {
		conn := clients[0].(*io.Socket)
		conn.On("benchmark", func(args ...any) {
			if len(args) > 0 {
				if ack, ok := args[len(args)-1].(func([]any, error)); ok {
					ack(nil, nil)
				}
			}
		})
	})

	httpServer := &http.Server{
		Addr:    ":3000",
		Handler: srv.ServeHandler(nil),
	}

	go func() {
		log.Println("Starting Target Server on :3000")
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server error: %v", err)
		}
	}()
	time.Sleep(1 * time.Second) // wait for server init

	// Memory Monitor
	go func() {
		for {
			var m runtime.MemStats
			runtime.ReadMemStats(&m)
			// Log in MBs
			fmt.Printf("\r[MEMORY] Alloc = %v MiB \t Sys = %v MiB \t NumGC = %v \t Goroutines = %v",
				m.Alloc/1024/1024, m.Sys/1024/1024, m.NumGC, runtime.NumGoroutine())
			time.Sleep(2 * time.Second)
		}
	}()

	log.Println("\nStarting Benchmark phase: high frequency connect/disconnect...")

	var wg sync.WaitGroup
	var activeConns int32
	var maxConns int32 = 200

	// We'll perform cycles of connection thrashing
	for cycle := 0; cycle < 5; cycle++ {
		wg.Add(1)
		go func(c int) {
			defer wg.Done()
			for i := int32(0); i < maxConns; i++ {
				atomic.AddInt32(&activeConns, 1)

				manager := io_client.NewManager("http://localhost:3000", nil)
				client := manager.Socket("/", nil)
				client.On("connect", func(...any) {
					// emit data and ask for ack, then disconnect
					client.EmitWithAck("benchmark", "data")(func(_ []any, err error) {
						client.Disconnect()
					})
				})
				client.On("connect_error", func(args ...any) {
					client.Disconnect()
				})
				client.Connect()

				// Wait slightly to not blow up ports instantly
				time.Sleep(5 * time.Millisecond)
			}
		}(cycle)

		// Space out batches
		time.Sleep(100 * time.Millisecond)
	}

	wg.Wait()
	log.Printf("\nAll benchmark connect/disconnect flows initiated. Waiting for garbage collection...\n")

	// Monitor cooldown phase
	for i := 0; i < 5; i++ {
		runtime.GC()
		time.Sleep(2 * time.Second)
	}

	fmt.Println("\nBenchmark Complete! You can let this run to check if Goroutines fully drop to baseline (approx ~10-20). Press Ctrl+C to exit.")

	// Wait for exit
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit

	_ = httpServer.Close()
	srv.Close(nil)
}
