package scv

import (
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"../util"
)

var _ = fmt.Printf

// A mechanism for allowing dependency injection. These methods will usually close other external app variables like DBs, file/io, etc.
type Injector interface {
	DeactivateStreamService(*Stream) error
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
	injector       Injector
	expirationTime int // how long to wait on each stream if no heartbeat (in minutes)
}

func NewManager(inj Injector) *Manager {
	m := Manager{
		targets:        make(map[string]*Target),
		streams:        make(map[string]*Stream),
		injector:       inj,
		expirationTime: 1200,
	}
	return &m
}

func createToken(targetId string) string {
	return targetId + ":" + util.RandSeq(36)
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
func (m *Manager) AddStream(stream *Stream, targetId string) error {
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
	t.Lock()
	defer t.Unlock()
	t.inactiveStreams.Add(stream)
	return nil
}

/*
Remove a stream from the manager. The stream is immediately removed. It is up to the calling function to specify clean-up behavior.
We need to lock the stream here because other functions may be using it.
*/
func (m *Manager) RemoveStream(streamId string) error {
	m.Lock()
	defer m.Unlock()
	stream, ok := m.streams[streamId]
	if ok == false {
		return errors.New("stream " + streamId + " does not exist")
	}
	t := m.targets[stream.TargetId]
	t.Lock()
	defer t.Unlock()
	stream.Lock()
	defer stream.Unlock()
	delete(m.streams, streamId)
	if stream.activeStream != nil {
		t.deactivateStreamImpl(stream)
	}
	t.inactiveStreams.Remove(stream)
	if len(t.activeStreams) == 0 && t.inactiveStreams.Len() == 0 {
		delete(m.targets, stream.TargetId)
	}
	return nil
}

func (m *Manager) ReadStream(streamId string, fn func(*Stream) error) error {
	m.RLock()
	stream, ok := m.streams[streamId]
	if ok == false {
		m.RUnlock()
		return errors.New("stream " + streamId + " does not exist")
	}
	t := m.targets[stream.TargetId]
	t.RLock()
	stream.RLock()
	t.RUnlock()
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
	t := m.targets[stream.TargetId]
	t.RLock()
	stream.Lock() // Acquire a write lock
	t.RUnlock()
	m.RUnlock()
	defer stream.Unlock()
	return fn(stream)
}

func (m *Manager) ModifyActiveStream(token string, fn func(*Stream) error) error {
	m.RLock()
	targetId := parseToken(token)
	if targetId == "" {
		m.RUnlock()
		return errors.New("invalid token: " + token)
	}
	t, ok := m.targets[targetId]
	if ok == false {
		m.RUnlock()
		return errors.New("invalid parsed target: " + targetId)
	}
	t.RLock()
	stream, ok := t.tokens[token]
	if ok == false {
		t.RUnlock()
		m.RUnlock()
		return errors.New("invalid token: " + token)
	}
	stream.Lock()
	// DeactivateStream/ActivateStreamHandler can muck around with other streams here, but just not this particular stream.
	t.RUnlock()
	m.RUnlock()
	defer stream.Unlock()
	return fn(stream)
}

func (m *Manager) ActivateStream(targetId, user, engine string) (token string, streamId string, err error) {
	// s_time := float64(time.Now().UnixNano()) / float64(1e9)
	m.RLock()
	// s_time_1 := float64(time.Now().UnixNano()) / float64(1e9)
	defer m.RUnlock()
	t, ok := m.targets[targetId]
	if ok == false {
		err = errors.New("Target does not exist")
		return
	}
	t.Lock()
	// s_time_2 := float64(time.Now().UnixNano()) / float64(1e9)
	defer t.Unlock()
	iterator := t.inactiveStreams.Iterator()
	ok = iterator.Next()
	if ok == false {
		err = errors.New("Target does not have streams")
		return
	}
	token = createToken(targetId)
	stream := iterator.Key().(*Stream)
	streamId = stream.StreamId
	stream.Lock()
	// s_time_3 := float64(time.Now().UnixNano()) / float64(1e9)
	defer stream.Unlock()
	t.inactiveStreams.Remove(stream)
	stream.activeStream = NewActiveStream(user, token, engine)
	t.tokens[token] = stream
	t.timers[stream.StreamId] = time.AfterFunc(time.Second*time.Duration(m.expirationTime), func() {
		m.DeactivateStream(stream.StreamId)
	})
	t.activeStreams[stream] = struct{}{}
	// s_time_4 := float64(time.Now().UnixNano()) / float64(1e9)
	// fmt.Printf("activa | m: %.2e t: %.2e s: %.2e fn: %.2e total: %.2e\n", s_time_1-s_time,
	// s_time_2-s_time_1, s_time_3-s_time_2, s_time_4-s_time_3, s_time_4-s_time)
	return
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

// Assumes that locks are in place.
func (m *Manager) deactivateStreamImpl(s *Stream, t *Target) {
	delete(t.tokens, s.activeStream.authToken)
	delete(t.timers, s.StreamId)
	delete(t.activeStreams, s)
	s.activeStream = nil
	t.inactiveStreams.Add(s)
}

func (m *Manager) DeactivateStream(streamId string) error {
	m.RLock()
	stream, ok := m.streams[streamId]
	// It's not the prettiest code when we can't use defer m.RUnlock, but it is performance critical.
	if ok == false {
		m.RUnlock()
		return errors.New("Stream does not exist")
	}
	if stream.activeStream == nil {
		m.RUnlock()
		return errors.New("Stream is not active")
	}
	t := m.targets[stream.TargetId]
	t.Lock()
	stream.Lock()
	defer stream.Unlock()
	m.RUnlock()
	t.deactivateStreamImpl(stream)
	t.Unlock()
	// this could potentially be a really long running service, so we don't want it to block the target or the manager
	return m.injector.DeactivateStreamService(stream)
}

func (m *Manager) LoadCheckpoints(dataDir, streamId string) (files map[string]string, err error) {
	m.RLock()
	defer m.RUnlock()
	stream := m.streams[streamId]
	stream.RLock()
	defer stream.RUnlock()
	streamDir := filepath.Join(dataDir, streamId)
	if stream.Frames > 0 {
		frameDir := filepath.Join(streamDir, strconv.Itoa(stream.Frames))
		checkpointDirs, e := ioutil.ReadDir(frameDir)
		if e != nil {
			return nil, err
		}
		// find the folder containing the last checkpoint
		lastCheckpoint := 0
		for _, fileProp := range checkpointDirs {
			count, _ := strconv.Atoi(fileProp.Name())
			if count > lastCheckpoint {
				lastCheckpoint = count
			}
		}
		checkpointDir := filepath.Join(frameDir, "checkpoint_files")
		checkpointFiles, e := ioutil.ReadDir(checkpointDir)
		if e != nil {
			return nil, e
		}
		for _, fileProp := range checkpointFiles {
			binary, e := ioutil.ReadFile(filepath.Join(checkpointDir, fileProp.Name()))
			if e != nil {
				return nil, e
			}
			files[fileProp.Name()] = string(binary)
		}
	}
	seedDir := filepath.Join(streamDir, "files")
	seedFiles, e := ioutil.ReadDir(seedDir)
	if e != nil {
		return nil, e
	}
	for _, fileProp := range seedFiles {
		// insert seedFile only if it's not already included from checkpoint
		_, ok := files[fileProp.Name()]
		if ok == false {
			binary, e := ioutil.ReadFile(filepath.Join(seedDir, fileProp.Name()))
			if e != nil {
				return nil, e
			}
			files[fileProp.Name()] = string(binary)
		}
	}
	return
}
