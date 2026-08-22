// Package queue provides a sequential, non-blocking task execution queue.
package queue

import (
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/aqcool/socket.io/v4/pkg/log"
)

var queueLog = log.NewLog("engine:events")

// EnqueueResult reports what happened to a submitted task.
type EnqueueResult uint8

const (
	EnqueueAccepted EnqueueResult = iota
	EnqueueClosed
	EnqueueFull
)

// Queue serializes function execution through a single goroutine.
//
// A MaxPending value of zero means unbounded. Production Socket.IO servers
// should normally configure a positive bound to prevent a slow handler from
// turning one connection into an unbounded memory queue.
type Queue struct {
	mu           sync.Mutex
	cond         *sync.Cond
	tasks        []func()
	shuttingDown bool
	done         chan struct{}
	maxPending   int
	overflows    atomic.Uint64
}

// New creates an unbounded queue. It remains for low-level callers that need
// the historical behavior. Socket.IO v4 configures a positive bound.
func New() *Queue {
	return NewBounded(0)
}

// NewBounded creates a queue with at most maxPending queued tasks. A value <= 0
// disables the bound.
func NewBounded(maxPending int) *Queue {
	if maxPending < 0 {
		maxPending = 0
	}
	capacity := maxPending
	if capacity <= 0 || capacity > 1024 {
		capacity = 1024
	}
	q := &Queue{
		tasks:      make([]func(), 0, capacity),
		done:       make(chan struct{}),
		maxPending: maxPending,
	}
	q.cond = sync.NewCond(&q.mu)
	go q.loop()
	return q
}

// SetMaxPending changes the pending-task limit. Zero means unbounded. Existing
// queued work is not discarded when a smaller limit is installed.
func (q *Queue) SetMaxPending(maxPending int) {
	if q == nil {
		return
	}
	if maxPending < 0 {
		maxPending = 0
	}
	q.mu.Lock()
	q.maxPending = maxPending
	q.mu.Unlock()
}

// MaxPending returns the configured pending-task limit. Zero means unbounded.
func (q *Queue) MaxPending() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxPending
}

// TryEnqueue adds a task without blocking and reports whether it was accepted.
func (q *Queue) TryEnqueue(task func()) EnqueueResult {
	if q == nil || task == nil {
		return EnqueueClosed
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.shuttingDown {
		return EnqueueClosed
	}
	if q.maxPending > 0 && len(q.tasks) >= q.maxPending {
		q.overflows.Add(1)
		return EnqueueFull
	}

	q.tasks = append(q.tasks, task)
	q.cond.Signal()
	return EnqueueAccepted
}

// Enqueue keeps the historical fire-and-forget surface. Call TryEnqueue when
// overflow behavior matters.
func (q *Queue) Enqueue(task func()) {
	_ = q.TryEnqueue(task)
}

// Size returns the number of pending tasks, excluding the task currently being
// executed.
func (q *Queue) Size() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

// OverflowCount reports how many submissions were rejected because the queue
// was full.
func (q *Queue) OverflowCount() uint64 {
	if q == nil {
		return 0
	}
	return q.overflows.Load()
}

func (q *Queue) loop() {
	defer close(q.done)
	for {
		task, ok := q.get()
		if !ok {
			return
		}
		q.execute(task)
	}
}

func (q *Queue) get() (func(), bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	for len(q.tasks) == 0 && !q.shuttingDown {
		q.cond.Wait()
	}
	if len(q.tasks) == 0 && q.shuttingDown {
		return nil, false
	}

	task := q.tasks[0]
	q.tasks[0] = nil
	q.tasks = q.tasks[1:]
	if len(q.tasks) == 0 {
		q.tasks = q.tasks[:0]
	}
	return task, true
}

func (q *Queue) execute(task func()) {
	defer func() {
		if r := recover(); r != nil {
			queueLog.Errorf("queue task panic recovered: %v\n%s", r, debug.Stack())
		}
	}()
	task()
}

// Close stops accepting new tasks, drains accepted tasks and waits for the
// consumer goroutine to exit.
func (q *Queue) Close() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.shuttingDown = true
	q.cond.Broadcast()
	q.mu.Unlock()
	<-q.done
}

func (q *Queue) IsShuttingDown() bool {
	if q == nil {
		return true
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.shuttingDown
}

// TryClose stops accepting new tasks without waiting for already accepted work
// to drain.
func (q *Queue) TryClose() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.shuttingDown = true
	q.cond.Broadcast()
	q.mu.Unlock()
}
