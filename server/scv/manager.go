package scv

import (
	"../util"
	"errors"
	"fmt"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var _ = fmt.Printf

// The mutex in Manager makes guarantees about the state of the system:
// 1. If the mutex (read or write) can be acquired, then there is no other concurrent operation that could affect stream creation, deletion, activation, or deactivation, target creation and deletion.
// 2. If you acquire a read lock, you still need to lock individual targets and streams when modifying them (eg. when posting frames).
// 3. Note that the stream or target may already have been removed by another operation, so it is important that you check the return value of anything you retrieve from the maps for existence. For example, it is possible that one goroutine is trying to deactivate the stream, while another goroutine is trying to post a frame. Both goroutines may be trying to acquire the lock at the same time. If the write goroutine acquires it first, then this means the read goroutine must verify the existence of the active stream through the token.
// 4. A target exists in the target map if and only if one or more of its streams exists in the streams map.

// A mechanism for allowing dependency injection. These methods will usually close other external app variables like DBs, file/io, etc.
type Manager struct {
	sync.RWMutex
	targets     map[string]*Target     // map of targetId to Target
	tokens      map[string]*Stream     // map of token to Stream
	streams     map[string]*Stream     // map of streamId to Stream
	timers      map[string]*time.Timer // map of stream timers
	deactivator func(*Stream) error    // a function that deactivates streams.
}

func NewManager(fn func(*Stream) error) *Manager {
	tm := Manager{
		targets:     make(map[string]*Target),
		tokens:      make(map[string]*Stream),
		streams:     make(map[string]*Stream),
		timers:      make(map[string]*time.Timer),
		deactivator: fn,
	}
	return &tm
}

// Add a stream to target targetId. If the target does not exist, the manager
// will create the target automatically. The manager also assumes ownership of
// the stream. Fn is a callback (typically a closure) executed at the end.
func (m *Manager) AddStream(stream *Stream, targetId string, fn func(*Stream) error) error {
	m.Lock()
	defer m.Unlock()
	// Create a target if it does not exist.
	_, ok := m.targets[targetId]
	if ok == false {
		m.targets[targetId] = NewTarget()
	}
	t := m.targets[targetId]
	m.streams[stream.streamId] = stream
	t.Lock()
	defer t.Unlock()
	t.inactiveStreams.Add(stream)
	return fn(stream)
}

// func (m *Manager) ReadStream(streamId, callback func() error) error {
// 	m.RLock()
// 	defer m.RUnlock()
// 	stream, ok := m.streams[streamId]
// 	if ok == false {
// 		return errors.New("stream " + streamId + " does not exist")
// 	}
// 	t := m.targets[stream.targetId]
// 	t.RLock()
// 	defer t.RUnlock()
// 	stream.RLock()
// 	defer stream.RUnlock()
// 	if callback != nil {
// 		return callback()
// 	}
// 	return nil
// }

// func (m *Manager) ModifyStream(streamId, callback func() error) error {
// 	m.RLock()
// 	defer m.RUnlock()
// 	stream, ok := m.streams[streamId]
// 	if ok == false {
// 		return errors.New("stream " + streamId + " does not exist")
// 	}
// 	t := m.targets[stream.targetId]
// 	t.Lock()
// 	defer t.Unlock()
// 	stream.Lock()
// 	defer stream.Unlock()
// 	if callback != nil {
// 		return callback()
// 	}
// 	return nil
// }

// Remove stream. If the stream is the last stream in the target, then the
// target is also removed.
func (m *Manager) RemoveStream(streamId string, fn func(*Stream) error) error {
	m.Lock()
	defer m.Unlock()
	stream, ok := m.streams[streamId]
	if ok == false {
		return errors.New("stream " + streamId + " does not exist")
	}
	t := m.targets[stream.targetId]
	t.Lock()
	defer t.Unlock()
	stream.Lock()
	defer stream.Unlock()
	delete(m.streams, streamId)
	delete(m.tokens, streamId)
	if stream.activeStream != nil {
		m.deactivateStreamImpl(stream, t)
	}
	t.inactiveStreams.Remove(stream)
	fmt.Println(t.activeStreams, "|", t.inactiveStreams.Len())
	if len(t.activeStreams) == 0 && t.inactiveStreams.Len() == 0 {
		delete(m.targets, stream.targetId)
	}
	return fn(stream)
}

func (m *Manager) ActivateStream(targetId, user, engine string) (token string, err error) {
	m.Lock()
	defer m.Unlock()
	t, ok := m.targets[targetId]
	if ok == false {
		err = errors.New("Target does not exist")
		return
	}
	t.Lock()
	defer t.Unlock()
	iterator := t.inactiveStreams.Iterator()
	ok = iterator.Next()
	if ok == false {
		err = errors.New("Target does not have streams")
		return
	}
	token = util.RandSeq(36)
	stream := iterator.Key().(*Stream)
	stream.Lock()
	defer stream.Unlock()
	t.inactiveStreams.Remove(stream)
	stream.activeStream = NewActiveStream(user, token, engine)
	m.tokens[token] = stream
	m.timers[stream.streamId] = time.AfterFunc(time.Second*time.Duration(t.ExpirationTime), func() {
		m.DeactivateStream(stream.streamId)
	})
	t.activeStreams[stream] = struct{}{}
	return
}

// Assumes that locks are in place.
func (m *Manager) deactivateStreamImpl(s *Stream, t *Target) {
	delete(m.tokens, s.activeStream.authToken)
	delete(m.timers, s.streamId)
	delete(t.activeStreams, s)
	s.activeStream = nil
	t.inactiveStreams.Add(s)
}

func (m *Manager) DeactivateStream(streamId string) error {
	m.Lock()
	defer m.Unlock()
	stream, ok := m.streams[streamId]
	if ok == false {
		return errors.New("Stream does not exist")
	}
	if stream.activeStream == nil {
		return errors.New("Stream is not active")
	}
	t := m.targets[stream.targetId]
	t.Lock()
	defer t.Unlock()
	stream.Lock()
	defer stream.Unlock()
	m.deactivateStreamImpl(stream, t)
	return m.deactivator(stream)
}

// func (m *Manager) ModifyStream(streamId string, fn func()) error {

// }

// func (m *Manager) DownloadFile(dataDir, streamId string) (files map[string]string, err error) {
// 	m.RLock()
// 	defer m.RUnlock()
// 	stream := m.streams[streamId]
// 	stream.RLock()
// 	defer stream.RUnlock()
// 	//
// }

func (m *Manager) LoadCheckpoints(dataDir, streamId string) (files map[string]string, err error) {
	m.RLock()
	defer m.RUnlock()
	stream := m.streams[streamId]
	stream.RLock()
	defer stream.RUnlock()
	streamDir := filepath.Join(dataDir, streamId)
	if stream.frames > 0 {
		frameDir := filepath.Join(streamDir, strconv.Itoa(stream.frames))
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
