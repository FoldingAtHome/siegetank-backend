package scv

import (
	"sync"
)

type Target struct {
	sync.RWMutex
	activeStreams   map[*Stream]struct{} // set of active streams
	inactiveStreams *Set                 // queue of inactive streams
	expirations     chan string          // expiration channel for timers
	ExpirationTime  int                  // expiration time in seconds
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
		activeStreams:   make(map[*Stream]struct{}),
		inactiveStreams: NewCustomSet(StreamComp),
		expirations:     make(chan string),
		ExpirationTime:  900,
	}
	return &target
}
