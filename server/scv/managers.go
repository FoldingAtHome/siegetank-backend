package scv

import (
	"errors"
	"sync"
)

// The mutex in Manager makes guarantees about the state of the system:
// 1. If the mutex (read or write) can be acquired, then there is no other concurrent operation that could affect stream creation, deletion, activation, or deactivation, target creation and deletion.
// 2. If you acquire a read lock, you still need to lock individual targets and streams when modifying them (eg. when posting frames).
// 3. Note that the stream or target may already have been removed by another operation, so it is important that you check the return value of anything you retrieve from the maps for existence. For example, it is possible that one goroutine is trying to deactivate the stream, while another goroutine is trying to post a frame. Both goroutines may be trying to acquire the lock at the same time. If the write goroutine acquires it first, then this means the read goroutine must verify the existence of the active stream through the token.
// 4. A target exists in the target map if and only if one or more of its streams exists in the streams map.

type Manager struct {
	sync.RWMutex
	targets map[string]*Target // map of targetId to Target
	tokens  map[string]*Stream // map of token to Stream
	streams map[string]*Stream // map of streamId to Stream
}

func NewManager() *Manager {
	tm := Manager{
		targets: make(map[string]*Target),
		tokens:  make(map[string]*Stream),
		streams: make(map[string]*Stream),
	}
	return &tm
}

// Guarantee:

// Add a stream to target targetId. If the target does not exist, the manager
// will create the target automatically. The manager also assumes ownership of
// the stream.
func (m *Manager) AddStream(stream *Stream, targetId string) {
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
	m.deactivateStreamImpl(stream)
	t.inactiveStreams.Add(stream)
}

// Remove stream. If the stream is the last stream in the target, then the
// target is also removed.
func (m *Manager) RemoveStream(streamId string) error {
	m.Lock()
	defer m.Unlock()
	stream, ok := m.streams[streamId]
	if ok == false {
		return errors.New("stream " + streamId + " does not exist")
	}
	t := m.targets[stream.targetId]
	t.Lock()
	defer t.Unlock()
	delete(m.streams, streamId)
	delete(m.tokens, streamId)
	m.deactivateStreamImpl(stream, t)
	t.inactiveStreams.Remove(stream)
	if len(t.activeStreams) == 0 && t.inactiveStreams.Len() == 0 {
		delete(m.targets, stream.targetId)
	}
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
	ok := iterator.Next()
	if ok == false {
		err = errors.New("Target does not have streams")
		return
	}
	stream := iterator.Key()
	stream.Lock()
	defer stream.Unlock()
	iterator.Remove(stream)
	token = util.RandSeq(36)
	stream.activeStream = NewActiveStream(user, token, engine)
	t.timers[stream.streamId] = time.AfterFunc(time.Second*time.Duration(t.ExpirationTime), func() {
		m.DeactivateStream(stream.streamId)
	})
	t.activeStreams[stream_id] = stream
}

// Assumes that locks are in place.
func (m *Manager) deactivateStreamImpl(stream *Stream, target *Target) {
	if stream.activeStream != nil {
		stream.activeStream = nil
		delete(m.tokens, stream.streamId)
		delete(t.activeStreams, stream.streamId)
		delete(t.timers, stream.streamId)
		target.inactiveStreams.Add(stream)
	}
}

func (m *Manager) DeactivateStream(streamId string) error {
	m.Lock()
	defer m.Unlock()
	stream, ok = m.tokens[streamId]
	if ok == false {
		return errors.New("Stream is not active")
	}
	t = m.targets[stream.targetId]
	t.Lock()
	defer t.Unlock()
	stream.Lock()
	defer stream.Unlock()
	m.deactivateStreamImpl(stream, t)
}

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
		checkpointDirs, err := ioutil.ReadDir(frameDir)
		if err != nil {
			return
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
		checkpointFiles, err := ioutil.ReadDir(checkpointDir)
		if err != nil {
			return
		}
		for _, fileProp := range checkpointFiles {
			binary, err := ioutil.ReadFile(filepath.Join(checkpointDir, fileProp.Name()))
			if err != nil {
				return
			}
			files[fileProp.Name()] = string(binary)
		}
	}
	seedDir := filepath.Join(streamDir, "files")
	seedFiles, err := ioutil.ReadDir(seedDir)
	if err != nil {
		return
	}
	for _, fileProp := range seedFiles {
		// insert seedFile only if it's not already included from checkpoint
		_, ok := files[fileProp.Name()]
		if ok == false {
			binary, err := ioutil.ReadFile(filepath.Join(seedDir, fileProp.Name()))
			if err != nil {
				return
			}
			files[fileProp.Name()] = string(binary)
		}
	}
}
