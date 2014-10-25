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

/*
Locks are structured such that:
-Methods on Streams require only its own lock.
-Methods on StreamManager require only its own lock.
-Methods on TokenManager require only its own lock.
-Methods on Targets may require its own lock, and all of the locks above.
*/

// func main() {
// 	a := &Stream{"a", 5}
// 	b := &Stream{"b", 2}
// 	c := &Stream{"c", 12}
// 	d := &Stream{"d", 12}
// 	s := NewCustomSet(func(l, r interface{}) bool {
// 		s1 := l.(*Stream)
// 		s2 := r.(*Stream)
// 		if s1.frames == s2.frames {
// 			return s1.id < s2.id
// 		} else {
// 			return s1.frames < s2.frames
// 		}
// 	})
// 	s.Add(a)
// 	s.Add(b)
// 	s.Add(c)
// 	s.Add(d)
// 	unboundIterator := s.Iterator()
// 	for unboundIterator.Next() {
// 		fmt.Printf("%d: %s\n", unboundIterator.Key(), unboundIterator.Value())
// 	}
// 	fmt.Println("----------")
// 	s.Remove(c)
// 	s.Remove(a)
// 	s.Remove(d)
// 	s.Remove(b)
// 	unboundIterator = s.Iterator()
// 	for unboundIterator.Next() {
// 		fmt.Printf("%d: %s\n", unboundIterator.Key(), unboundIterator.Value())
// 	}
// }

type Target struct {
	CommandQueue
	targetId        string                 // id of target
	inactiveStreams *SkipList              // queue of inactive streams
	activeStreams   map[*Stream]struct{}   // set of active streams
	timers          map[string]*time.Timer // expiration timer for each stream
	expirations     chan string            // expiration channel for timers
	ExpirationTime  int                    // expiration time in seconds
	targetManager   *TargetManager
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

func NewTarget(tm *TargetManager) *Target {
	target := Target{
		CommandQueue:    CommandQueue{},
		inactiveStreams: NewCustomMap(StreamComp),
		activeStreams:   make(map[*Stream]struct{}),
		timers:          make(map[*time.Timer]struct{}),
		expirations:     make(chan string),
		ExpirationTime:  600,
		targetManager:   tm,
	}
	return &target
}

func (t *Target) AddStream(stream *Stream) error {
	return t.Dispatch(func() {
		item := &QueueItem{value: stream}
		t.inactiveStreams.Add(stream)
		t.targetManager.AddStream(stream.streamId, stream)
	})
}

func (t *Target) RemoveStream(stream *Stream) error {
	return t.Dispatch(func() {
		t.targetManager.RemoveStream(stream.streamId)
		t.deactivateImpl(stream_id)
		t.inactiveStreams.Remove(stream)
		stream.Die()
	})
}

func (t *Target) ActivateStream(user, engine string) (token, stream_id string, err error) {
	err2 := t.Dispatch(func() {
		t.targetManager.Lock()
		defer t.targetManager.Unlock()
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
		t.targetManager.tokens[token] = stream
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

func (t *Target) deactivateImpl(stream *Stream) {
	t.targetManager.RemoveToken(stream.authToken)
	stream.Deactivate()
	item := &QueueItem{value: stream}
	delete(target.activeStreams, stream)
	heap.Push(&target.inactiveStreams, item)
	delete(target.timers, stream_id)
}

func (t *Target) DeactivateStream(stream *Stream) error {
	err2 := t.Dispatch(func() {
		target.targetManager.Lock()
		defer target.targetManager.Unlock()
		t.deactivateImpl(stream)
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

func (t *Target) ActiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for k := range t.activeStreams {
			copy[k.streamId] = struct{}{}
		}
	})
	return
}

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
// 			t.deactivateImpl(stream_id)
// 		case <-t.finished:
// 			return
// 		}
// 	}
// }

func (t *Target) Die() {
	close(t.finished)
}
