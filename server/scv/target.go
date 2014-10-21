package scv

import (
	"../util"
	"container/heap"
	"errors"
	"fmt"
	"sync"
	"time"
)

var _ = fmt.Printf

type ActiveStream struct {
	sync.RWMutex
	entireFrames float64 // number of frames across all donors
	totalFrames  float64 // number of frames done by this donor
	bufferFrames int     // number of frames stored in the buffer
	authToken    string
	user         string
	startTime    int
	frameHash    string
	engine       string
}

type Target struct {
	CommandQueue
	inactiveStreams PriorityQueue
	activeStreams   map[string]*ActiveStream
	timers          map[string]*time.Timer // map of timers
	expirations     chan string            // expiration channel for the timers
	ExpirationTime  int                    // expiration time in seconds
	targetManager   *TargetManager
}

func NewTarget(tm *TargetManager) *Target {
	target := Target{
		CommandQueue:    makeCommandQueue(),
		inactiveStreams: make(PriorityQueue, 0),
		activeStreams:   make(map[string]*ActiveStream),
		timers:          make(map[string]*time.Timer),
		expirations:     make(chan string),
		ExpirationTime:  600,
		targetManager:   tm,
	}
	heap.Init(&target.inactiveStreams)
	go func() {
		target.run()
	}()
	return &target
}

func (t *Target) AddStream(stream_id string, weight float64) error {
	return t.Dispatch(func() {
		item := &QueueItem{
			value:    stream_id,
			priority: weight,
		}
		heap.Push(&t.inactiveStreams, item)
	})
}

func (t *Target) RemoveStream(stream_id string) error {
	return t.Dispatch(func() {
		t.deactivate(stream_id)
		heap.Pop(&t.inactiveStreams)
	})
}

func (t *Target) ActivateStream(user, engine string) (token, stream_id string, err error) {
	err2 := t.Dispatch(func() {
		// TODO: Change to a queue
		for _, qItem := range t.inactiveStreams {
			stream_id = qItem.value
			break
		}
		if stream_id == "" {
			err = errors.New("No streams can be activated.")
			return
		}
		token = util.RandSeq(36)
		as := &ActiveStream{
			user:      user,
			engine:    engine,
			authToken: token,
		}
		t.timers[stream_id] = time.AfterFunc(time.Second*time.Duration(t.ExpirationTime), func() {
			t.expirations <- stream_id
		})
		t.activeStreams[stream_id] = as
		t.targetManager.Tokens.AddToken(token, as)
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

func (t *Target) DeactivateStream(stream_id string) error {
	return t.Dispatch(func() {
		t.deactivate(stream_id)
	})
}

func (t *Target) ActiveStream(stream_id string) (as *ActiveStream, err error) {
	var ok bool
	err2 := t.Dispatch(func() {
		as, ok = t.activeStreams[stream_id]
	})
	if err2 != nil {
		return nil, err2
	}
	if ok == false {
		return nil, errors.New("Stream is not active")
	}
	return
}

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe. If the caller would like to
// iterate over the list of streams returned in here, he'd need to call Lock
func (t *Target) ActiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for k := range t.activeStreams {
			copy[k] = struct{}{}
		}
	})
	return
}

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe.
func (t *Target) InactiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for _, item := range t.inactiveStreams {
			copy[item.value] = struct{}{}
		}
	})
	return copy, err
}

func (t *Target) run() {
	for {
		select {
		case stream_id := <-t.expirations:
			t.deactivate(stream_id)
		case msg := <-t.commands:
			msg.fn()
			msg.wait <- struct{}{}
		case <-t.finished:
			return
		}
	}
}

func (target *Target) deactivate(stream_id string) {
	prop, ok := target.activeStreams[stream_id]
	if ok {
		target.targetManager.Tokens.RemoveToken(prop.authToken)
		item := &QueueItem{
			value:    stream_id,
			priority: target.activeStreams[stream_id].entireFrames,
		}
		delete(target.activeStreams, stream_id)
		heap.Push(&target.inactiveStreams, item)
		delete(target.timers, stream_id)
	}
}
