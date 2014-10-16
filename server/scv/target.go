package scv

import (
	// "fmt"
	"errors"
	"sync"
	"time"
	"../util"
)

type ActiveStream struct {
    sync.RWMutex
	totalFrames  float32
	bufferFrames int
	authToken    string
	user          string
	startTime    int
	frameHash    string
	engine        string
}

type Target struct {
	CommandQueue
    inactiveStreams map[string]struct{}
    activeStreams   map[string]*ActiveStream
	timers          map[string]*time.Timer // map of timers
	expirations     chan string            // expiration channel for the timers
	expirationTime  int                    // expiration time in seconds
    targetManager   *TargetManager
}

func NewTarget(tm *TargetManager) *Target {
	target := Target{
		CommandQueue: makeCommandQueue(),
		activeStreams:   make(map[string]*ActiveStream),
		inactiveStreams: make(map[string]struct{}),
		timers:           make(map[string]*time.Timer),
		expirations:      make(chan string),
        expirationTime:  600,
		targetManager:    tm,
	}
	go func() {
		target.run()
	}()
	return &target
}

func (t *Target) AddStream(stream_id string) error {
	return t.Dispatch(func(){
		t.inactiveStreams[stream_id] = struct{}{}
	})
}

func (t *Target) RemoveStream(stream_id string) error {
	return t.Dispatch(func(){
		delete(t.inactiveStreams, stream_id)
	})
}

func (t *Target) ActivateStream(user, engine string) (token, stream_id string, err error) {
	err = t.Dispatch(func(){
		for stream_id = range t.inactiveStreams {
			break
		}
		token = util.RandSeq(5)
		as := &ActiveStream{
			user:       user,
			engine:     engine,
			authToken: token,
		}
		t.timers[stream_id] = time.AfterFunc(time.Second*time.Duration(t.expirationTime), func() {
			t.expirations <- stream_id
		})
		t.activeStreams[stream_id] = as
		t.targetManager.Tokens.AddToken(token, as)
		delete(t.inactiveStreams, stream_id)
	})
	return
}

func (t *Target) DeactivateStream(stream_id string) error {
	return t.Dispatch(func(){
		t.deactivate(stream_id)
	})
}

func (t *Target) ActiveStream(stream_id string) (as *ActiveStream, err error) {
	var ok bool
	err2 := t.Dispatch(func(){
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
func (t *Target) GetInactiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for k, v := range t.inactiveStreams {
			copy[k] = v
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
            case <- t.finished:
                return
        }
    }
}

func (target *Target) deactivate(stream_id string) {
	prop, ok := target.activeStreams[stream_id]
	if ok {
		target.targetManager.Tokens.RemoveToken(prop.authToken)
		delete(target.activeStreams, stream_id)
		target.inactiveStreams[stream_id] = struct{}{}
		delete(target.timers, stream_id)
	}
}
