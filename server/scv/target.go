package scv

import (
	"sync"
	"time"
)

type Target struct {
	sync.RWMutex
	tokens          map[string]*Stream   // map of token to Stream
	activeStreams   map[*Stream]struct{} // set of active streams
	inactiveStreams *Set                 // queue of inactive streams
	timers          map[string]*time.Timer
	// ExpirationTime  int // expiration time in seconds
}

func StreamComp(l, r interface{}) bool {
	s1 := l.(*Stream)
	s2 := r.(*Stream)
	if s1.frames == s2.frames {
		return s1.streamId > s2.streamId
	} else {
		return s1.frames > s2.frames
	}
}

func NewTarget() *Target {
	target := Target{
		tokens:          make(map[string]*Stream),
		activeStreams:   make(map[*Stream]struct{}),
		inactiveStreams: NewCustomSet(StreamComp),
		timers:          make(map[string]*time.Timer),
	}
	return &target
}
