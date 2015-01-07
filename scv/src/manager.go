package scv

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

var _ = fmt.Printf

const MAX_STREAM_FAILS int = 50
const STREAM_EXPIRATION_TIME int = 1200

type Injector interface {
	DeactivateStreamService(*Stream) error // need to finish fast
	DisableStreamService(*Stream) error    // need to finish fast
	EnableStreamService(*Stream) error
}

// The mutex in Manager makes guarantees about the state of the system:
// 1. If the mutex (read or write) can be acquired, then there is no other concurrent operation that could affect stream creation, deletion, activation, or deactivation, target creation and deletion.
// 2. If you acquire a read lock, you still need to lock individual targets and streams when modifying them (eg. when posting frames).
// 3. Note that the stream or target may already have been removed by another operation, so it is important that you check the return value of anything you retrieve from the maps for existence. For example, it is possible that one goroutine is trying to deactivate the stream, while another goroutine is trying to post a frame. Both goroutines may be trying to acquire the lock at the same time. If the write goroutine acquires it first, then this means the read goroutine must verify the existence of the active stream through the token.
// 4. A target exists in the target map if and only if one or more of its streams exists in the streams map.
type Manager struct {
	sync.RWMutex
	targets        map[string]*Target // map of targetId to Target
	streams        map[string]*Stream // map of streamId to Stream
	tokens         map[string]*Stream // map of tokens to Stream
	injector       Injector
	expirationTime int
}

func NewManager(inj Injector) *Manager {
	m := Manager{
		targets:        make(map[string]*Target),
		streams:        make(map[string]*Stream),
		tokens:         make(map[string]*Stream),
		injector:       inj,
		expirationTime: STREAM_EXPIRATION_TIME,
	}
	return &m
}

func createToken(targetId string) string {
	return RandSeq(36)
}

func parseToken(token string) string {
	result := strings.Split(token, ":")
	if len(result) < 2 {
		return ""
	} else {
		return result[0]
	}
}

/*
Add a stream to the manager. If the stream exists, then nothing happens. Otherwise, if the target does not exist, a target
is created for this stream. It is assumed that the respective persistent structures (dbs, files) for this
stream has already been created and ready to go. It is assumed that while AddStream is called, no other goroutine is manipulating
this particular stream pointer.
*/
func (m *Manager) AddStream(stream *Stream, targetId string, enabled bool) error {
	m.Lock()
	defer m.Unlock()
	_, ok := m.streams[stream.StreamId]
	if ok == true {
		return errors.New("stream " + stream.StreamId + " already exists")
	}
	m.streams[stream.StreamId] = stream
	_, ok = m.targets[targetId]
	if ok == false {
		m.targets[targetId] = NewTarget()
	}
	t := m.targets[targetId]
	if enabled {
		t.inactiveStreams.Add(stream)
	} else {
		t.disabledStreams[stream] = struct{}{}
	}
	return nil
}

/*
Remove a stream from the manager. The stream is immediately removed from memory.
However, its data (including files and what not) still persist on disk. It is up
to the caller to take care of subsequent cleanup (if any). Returns true if target was removed.
*/
func (m *Manager) RemoveStream(streamId, user string) error {
	m.Lock()
	defer m.Unlock()
	stream, ok := m.streams[streamId]
	if ok == false {
		return errors.New("stream " + streamId + " does not exist")
	}
	if user != stream.Owner {
		return errors.New(user + " does not own stream " + streamId)
	}
	t := m.targets[stream.TargetId]
	stream.Lock()
	defer stream.Unlock()
	delete(m.streams, streamId)
	if stream.activeStream != nil {
		m.deactivateStreamImpl(stream, t)
	}
	// this is no longer a state transfer but a complete deletion
	t.inactiveStreams.Remove(stream)
	delete(t.disabledStreams, stream)
	if len(t.activeStreams) == 0 && t.inactiveStreams.Len() == 0 && len(t.disabledStreams) == 0 {
		delete(m.targets, stream.TargetId)
	}
	return nil
}

func (m *Manager) stateTransfer(s *Stream, src interface{}, dst interface{}) {

	// invariant:

	a := m.targets[s.TargetId].inactiveStreams.Contains(s)
	_, b := m.targets[s.TargetId].activeStreams[s]
	_, c := m.targets[s.TargetId].disabledStreams[s]

	// ensure that the stream must be in one of these states
	if ((a != b != c) && !(a && b && c)) == false {
		panic(fmt.Sprintf("stream state machine failed! a:%v, b:%v, c:%v", a, b, c))
	}

	switch v := src.(type) {
	case map[*Stream]struct{}:
		delete(v, s)
	case *Set:
		v.Remove(s)
	}
	switch w := dst.(type) {
	case map[*Stream]struct{}:
		w[s] = struct{}{}
	case *Set:
		w.Add(s)
	}
}

// Remove the stream from the active queue. Assumes that locks are in place for target and stream.
// If this function returns true, you are expected to call the corresponding injector.DeactivateStreamService()
func (m *Manager) deactivateStreamImpl(s *Stream, t *Target) {
	if s.activeStream != nil {
		delete(m.tokens, s.activeStream.authToken)
		s.activeStream.timer.Stop()
		m.injector.DeactivateStreamService(s)
		s.activeStream = nil
		m.stateTransfer(s, t.activeStreams, t.inactiveStreams)
	} else {
		panic("tried to deactivate an non-active stream")
	}
}

// Disables the stream entirely. Assumes that locks are in place for target and stream.
func (m *Manager) disableStreamImpl(stream *Stream, t *Target) {
	_, isDisabled := t.disabledStreams[stream]
	if isDisabled {
		return
	}
	stream.MongoStatus = "disabled"
	m.stateTransfer(stream, t.inactiveStreams, t.disabledStreams)
}

// Idempotent, does nothing if stream is already disabled. The stream service is still called!
func (m *Manager) DisableStream(streamId, user string) error {
	m.Lock()
	stream, ok := m.streams[streamId]
	if ok == false {
		m.Unlock()
		return errors.New("stream " + streamId + " does not exist")
	}
	stream.Lock()
	defer stream.Unlock()
	if user != stream.Owner {
		m.Unlock()
		return errors.New("you do not own this stream.")
	}
	t := m.targets[stream.TargetId]
	// state transfers to inactive if the stream is active
	isActive := (stream.activeStream != nil)
	if isActive {
		m.deactivateStreamImpl(stream, t)
	}
	// state transfer from inactive to disabled
	m.disableStreamImpl(stream, t)
	m.Unlock()
	return m.injector.DisableStreamService(stream)
}

func (m *Manager) EnableStream(streamId, user string) error {
	m.Lock()
	stream, ok := m.streams[streamId]
	if ok == false {
		m.Unlock()
		return errors.New("stream " + streamId + " does not exist")
	}
	stream.Lock()
	defer stream.Unlock()
	if user != stream.Owner {
		m.Unlock()
		return errors.New("you do not own this stream.")
	}
	t := m.targets[stream.TargetId]
	_, isActive := t.activeStreams[stream]
	isInactive := t.inactiveStreams.Contains(stream)
	if isActive || isInactive {
		m.Unlock()
		return m.injector.EnableStreamService(stream)
	}
	m.stateTransfer(stream, t.disabledStreams, t.inactiveStreams)
	m.Unlock()
	return m.injector.EnableStreamService(stream)
}

func (m *Manager) ReadStream(streamId string, fn func(*Stream) error) error {
	m.RLock()
	stream, ok := m.streams[streamId]
	if ok == false {
		m.RUnlock()
		return errors.New("stream " + streamId + " does not exist")
	}
	stream.RLock()
	m.RUnlock()
	defer stream.RUnlock()
	return fn(stream)
}

func (m *Manager) ModifyStream(streamId string, fn func(*Stream) error) error {
	m.RLock()
	stream, ok := m.streams[streamId]
	if ok == false {
		m.RUnlock()
		return errors.New("stream " + streamId + " does not exist")
	}
	stream.Lock() // Acquire a write lock
	defer stream.Unlock()
	m.RUnlock()
	return fn(stream)
}

func (m *Manager) GetActiveStreams() interface{} {
	m.RLock()
	finalized := map[string]interface{}{}
	for _, stream := range m.tokens {
		result := map[string]interface{}{}
		stream.RLock()
		result["donor_frames"] = stream.activeStream.donorFrames
		result["buffer_frames"] = stream.activeStream.bufferFrames
		result["user"] = stream.activeStream.user
		result["start_time"] = stream.activeStream.startTime
		result["engine"] = stream.activeStream.engine
		finalized[stream.StreamId] = result
		stream.RUnlock()
	}
	m.RUnlock()
	return finalized
}

func (m *Manager) ModifyActiveStream(token string, fn func(*Stream) error) error {
	m.RLock()
	stream, ok := m.tokens[token]
	if ok == false {
		m.RUnlock()
		return errors.New("invalid token: " + token)
	}
	stream.Lock()
	defer stream.Unlock()
	m.RUnlock()
	return fn(stream)
}

func (m *Manager) ResetActiveStream(token string) error {
	m.RLock()
	defer m.RUnlock()
	stream, ok := m.tokens[token]
	if ok == false {
		return errors.New("invalid token: " + token)
	}
	stream.Lock()
	defer stream.Unlock()
	stream.activeStream.timer.Reset(time.Duration(m.expirationTime) * time.Second)
	return nil
}

func (m *Manager) ActivateStream(targetId, user, engine string, fn func(*Stream) error) (token string, streamId string, err error) {
	m.Lock()

	t, ok := m.targets[targetId]
	if ok == false {
		m.Unlock()
		err = errors.New("Target does not exist")
		return
	}
	iterator := t.inactiveStreams.Iterator()
	ok = iterator.Next()
	if ok == false {
		m.Unlock()
		err = errors.New("Target does not have streams")
		return
	}
	token = createToken(targetId)
	stream := iterator.Key().(*Stream)
	streamId = stream.StreamId
	m.stateTransfer(stream, t.inactiveStreams, t.activeStreams)
	stream.activeStream = NewActiveStream(user, token, engine)
	m.tokens[token] = stream
	stream.activeStream.timer = time.AfterFunc(time.Second*time.Duration(m.expirationTime), func() {
		m.DeactivateStream(token, 0)
	})

	stream.Lock()
	defer stream.Unlock()
	m.Unlock()
	err = fn(stream)
	return
}

func (m *Manager) DeactivateStream(token string, error_count int) error {
	m.Lock()
	stream, ok := m.tokens[token]
	if ok == false {
		m.Unlock()
		return errors.New("invalid token: " + token)
	}
	t := m.targets[stream.TargetId]
	stream.Lock()
	defer stream.Unlock()
	stream.ErrorCount += error_count
	m.deactivateStreamImpl(stream, t)
	if stream.ErrorCount >= MAX_STREAM_FAILS {
		m.disableStreamImpl(stream, t)
		// we don't need to call DisableStreamService because DeactivateStreamService takes care of it.
	}
	m.Unlock()
	return nil
}

// // This returns by copy
// func (m *Manager) ActiveStreams(targetId string) (result map[ActiveStream]struct{}, err error) {
// 	m.RLock()
// 	defer m.RUnlock()
// 	t, ok := m.targets[targetId]
// 	if ok == false {
// 		err = errors.New("Target does not exist")
// 		return
// 	}
// 	t.RLock()
// 	defer t.RUnlock()
// 	result = make(map[ActiveStream]struct{})
// 	for token := range t.tokens {
// 		result[*t.tokens[token].activeStream] = struct{}{}
// 	}
// 	return
// }
