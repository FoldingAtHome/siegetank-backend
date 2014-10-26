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
	sync.Mutex
	activeStreams   map[*Stream]struct{}   // set of active streams
	inactiveStreams *SkipList              // queue of inactive streams
	timers          map[string]*time.Timer // expiration timer for each stream
	expirations     chan string            // expiration channel for timers
	ExpirationTime  int                    // expiration time in seconds
}

func StreamComp(l, r interface{}) bool {
	s1 := l.(*Stream)
	s2 := r.(*Stream)
	if s1.frames == s2.frames {
		return s1.id < s2.id
	} else {
		return s1.frames < s2.frames
	}
}

func NewTarget() *Target {
	target := Target{
		activeStreams:   make(map[*Stream]struct{}),
		inactiveStreams: NewCustomMap(StreamComp),
		timers:          make(map[*time.Timer]struct{}),
		expirations:     make(chan string),
		ExpirationTime:  600,
	}
	return &target
}
