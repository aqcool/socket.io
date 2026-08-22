package socketio

import (
	"errors"
	"runtime/debug"
	"sync"
	"sync/atomic"
)

var errQueueFull = errors.New("socket.io: socket event queue is full")

type dispatchQueue struct {
	mu      sync.Mutex
	cond    *sync.Cond
	tasks   []func()
	max     int
	policy  OverflowPolicy
	closed  bool
	started bool
	done    chan struct{}

	overflows atomic.Uint64
	onOverflow func(OverflowPolicy)
}

func newDispatchQueue(opts QueueOptions, onOverflow func(OverflowPolicy)) *dispatchQueue {
	q := &dispatchQueue{
		max:        opts.MaxPending,
		policy:     opts.Overflow,
		done:       make(chan struct{}),
		onOverflow: onOverflow,
	}
	capacity := opts.MaxPending
	if capacity <= 0 {
		capacity = 64
	}
	q.tasks = make([]func(), 0, capacity)
	q.cond = sync.NewCond(&q.mu)
	return q
}

// Start is deliberately separate from construction. Pre-connect sockets do
// not own a queue goroutine until namespace middleware has accepted them.
func (q *dispatchQueue) Start() {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.started || q.closed {
		q.mu.Unlock()
		return
	}
	q.started = true
	q.mu.Unlock()
	go q.loop()
}

func (q *dispatchQueue) Enqueue(task func()) error {
	if q == nil || task == nil {
		return nil
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return ErrClosed
	}
	if !q.started {
		q.mu.Unlock()
		return ErrNotConnected
	}
	if q.max > 0 && len(q.tasks) >= q.max {
		q.overflows.Add(1)
		policy := q.policy
		q.mu.Unlock()
		if q.onOverflow != nil {
			q.onOverflow(policy)
		}
		switch policy {
		case OverflowDropNewest:
			return nil
		case OverflowDisconnect, OverflowReject:
			return errQueueFull
		default:
			return errQueueFull
		}
	}
	q.tasks = append(q.tasks, task)
	q.cond.Signal()
	q.mu.Unlock()
	return nil
}

func (q *dispatchQueue) Pending() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.tasks)
}

func (q *dispatchQueue) Overflows() uint64 {
	if q == nil {
		return 0
	}
	return q.overflows.Load()
}

func (q *dispatchQueue) Close(dropPending bool) {
	if q == nil {
		return
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	if dropPending {
		clear(q.tasks)
		q.tasks = q.tasks[:0]
	}
	started := q.started
	q.cond.Broadcast()
	q.mu.Unlock()
	if started {
		<-q.done
	}
}

func (q *dispatchQueue) loop() {
	defer close(q.done)
	for {
		q.mu.Lock()
		for len(q.tasks) == 0 && !q.closed {
			q.cond.Wait()
		}
		if len(q.tasks) == 0 && q.closed {
			q.mu.Unlock()
			return
		}
		task := q.tasks[0]
		q.tasks[0] = nil
		q.tasks = q.tasks[1:]
		if len(q.tasks) == 0 {
			q.tasks = q.tasks[:0]
		}
		q.mu.Unlock()

		func() {
			defer func() {
				if recover() != nil {
					_ = debug.Stack()
				}
			}()
			task()
		}()
	}
}
