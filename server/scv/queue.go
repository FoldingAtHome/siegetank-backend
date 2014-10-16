package scv

import (
	"errors"
	"math/rand"
)

func RandSeq(n int) string {
	b := make([]rune, n)
	var letters = []rune("012345689ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type _message struct {
	fn   func()
	wait chan struct{}
}

type CommandQueue struct {
	commands chan _message
	finished chan struct{}
}

func makeCommandQueue() CommandQueue {
	q := CommandQueue{make(chan _message), make(chan struct{})}
	return q
}

func (q *CommandQueue) Die() {
	close(q.finished)
}

// Functions to be dispatched must not return anything. Use closures to retrieve data back out.
func (q *CommandQueue) Dispatch(fn func()) (err error) {
	msg := _message{fn, make(chan struct{})}
	select {
	case q.commands <- msg:
		<-msg.wait
	case <-q.finished:
		err = errors.New("No longer accepting new tasks.")
	}
	return
}

/* Example Implementation of Run()
func (q *CommandQueue) Run() {
    for {
        select {
            case msg := <-q.commands:
                msg.fn()
                msg.wait <- struct{}{}
            case <- q.finished:
                fmt.Println("Closing channel")
                return
        }
    }
}
*/
