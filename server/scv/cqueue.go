/* Command Queue for dealing with asynchronous methods on a locked object*/
package main

import (
	// "errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	// "sync/atomic"
	"errors"
	"time"
)

type CommandQueue struct {
	sync.Mutex
	finished bool
}

func (q *CommandQueue) Dispatch(fn func()) (err error) {
	q.Lock()
	if q.finished {
		return errors.New("No longer accepting new commands")
	}
	fn()
	q.Unlock()
	return
}

func (q *CommandQueue) Die() {
	q.Lock()
	q.finished = true
	q.Unlock()
}
