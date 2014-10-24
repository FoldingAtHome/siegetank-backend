package scv

import (
	"../util"
	"container/heap"
	"errors"
	"fmt"
	"io/ioutil"
	// "sort"
	"path/filepath"
	"strconv"
	"time"
)

var _ = fmt.Printf

type Target struct {
	CommandQueue
	targetId        string                 // id of target
	inactiveStreams PriorityQueue          // queue of inactive streams
	activeStreams   map[*Stream]struct{}   // set of active streams
	timers          map[string]*time.Timer // expiration timer for each stream
	expirations     chan string            // expiration channel for timers
	ExpirationTime  int                    // expiration time in seconds
	targetManager   *TargetManager
}

func NewTarget(tm *TargetManager) *Target {
	target := Target{
		CommandQueue:    CommandQueue{},
		inactiveStreams: make(PriorityQueue, 0),
		activeStreams:   make(map[*Stream]struct{}),
		timers:          make(map[*time.Timer]struct{}),
		expirations:     make(chan string),
		ExpirationTime:  600,
		targetManager:   tm,
	}
	heap.Init(&target.inactiveStreams)
	return &target
}

func (t *Target) AddStream(stream *Stream) error {
	return t.Dispatch(func() {
		t.targetManager.Streams.Lock()
		defer t.targetManager.Streams.Unlock()
		item := &QueueItem{
			value: stream,
		}
		heap.Push(&t.inactiveStreams, item)
		t.targetManager.Streams.AddStream(stream.streamId, stream)
	})
}

func (t *Target) RemoveStream(stream *Stream) error {
	return t.Dispatch(func() {
		// This method acquires four mutexes in this order:
		// 1. Target
		// 2. StreamManager
		// 3. TokenManager
		// 4. Stream
		// We need to lock all four because we don't want any other service
		// to potentially access this stream in any way while it's being removed.
		// Acquiring the stream lock itself may be the slowest if it's during
		// an IO operation, but everything else is fast.
		t.targetManager.Streams.Lock()
		defer t.targetManager.Streams.Unlock()
		t.deactivate(stream_id)
		heap.Pop(&t.inactiveStreams)
		stream.Die()
		t.targetManager.Streams.RemoveStream(stream.streamId)
	})
}

func (t *Target) ActivateStream(user, engine string) (token, stream_id string, err error) {
	err2 := t.Dispatch(func() {
		t.targetManager.Tokens.Lock()
		defer t.targetManager.Tokens.Unlock()
		var stream *Stream
		for _, stream = range t.inactiveStreams {
			break
		}
		if stream == nil {
			err = errors.New("No stream can be activated.")
			return
		}
		stream_id = stream.streamId
		token = util.RandSeq(36)
		stream.Activate(user, token, engine)
		t.timers[stream.streamId] = time.AfterFunc(time.Second*time.Duration(t.ExpirationTime), func() {
			t.DeactivateStream(stream)
		})
		t.activeStreams[stream_id] = stream
		t.targetManager.Tokens.AddToken(token, stream)
		heap.Pop(&t.inactiveStreams)
	})
	if err2 != nil {
		err = err2
		return
	}
	if err != nil {
		return
	}
	return
}

func (t *Target) deactivate(stream *Stream) {
	t.targetManager.Tokens.Lock()
	defer t.targetManager.Tokens.Unlock()
	stream.Deactivate()
	target.targetManager.Tokens.RemoveToken(stream.authToken)
	item := &QueueItem{
		value: stream,
	}
	delete(target.activeStreams, stream)
	heap.Push(&target.inactiveStreams, item)
	delete(target.timers, stream_id)
}

func (t *Target) DeactivateStream(stream *Stream) error {
	err2 := t.Dispatch(func() {
		t.deactivate(stream)
	})
	return err2
}

// func (t *Target) ActiveStream(stream_id string) (as *ActiveStream, err error) {
// 	var ok bool
// 	err2 := t.Dispatch(func() {
// 		as, ok = t.activeStreams[stream_id]
// 	})
// 	if err2 != nil {
// 		return nil, err2
// 	}
// 	if ok == false {
// 		return nil, errors.New("Stream is not active")
// 	}
// 	return
// }

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe. If the caller would like to
// iterate over the list of streams returned in here, he'd need to call Lock
func (t *Target) ActiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for k := range t.activeStreams {
			copy[k.streamId] = struct{}{}
		}
	})
	return
}

// Returns a copy. Adding to this mp will not affect the actual map used by
// the target since maps are not goroutine safe.
func (t *Target) InactiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for _, item := range t.inactiveStreams {
			copy[item.value.streamId] = struct{}{}
		}
	})
	return copy, err
}

// func (t *Target) run() {
// 	for {
// 		select {
// 		case stream_id := <-t.expirations:
// 			t.deactivate(stream_id)
// 		case <-t.finished:
// 			return
// 		}
// 	}
// }

func (t *Target) Die() {
	close(t.finished)
}
