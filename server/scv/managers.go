package scv

import (
	"errors"
	"sync"
)

// // map of tokens to active streams
// type TokenManager struct {
// 	sync.RWMutex
// 	tokens map[string]*Stream
// }

// // Not coroutine safe. You must lock yourself.
// func (t *TokenManager) AddToken(token string, stream *ActiveStream) {
// 	t.tokens[token] = stream
// }

// // Not coroutine safe. You must lock yourself.
// func (t *TokenManager) RemoveToken(token string) {
// 	delete(t.tokens, token)
// }

// func (t *TokenManager) FindStream(token string) (*ActiveStream, error) {
// 	t.RLock()
// 	defer t.RUnlock()
// 	stream, ok := t.tokens[token]
// 	if ok {
// 		return stream, nil
// 	} else {
// 		return nil, errors.New("Bad Token")
// 	}
// }

// // StreamManager maps stream_ids to *Stream
// type StreamManager struct {
// 	sync.RWMutex
// 	streams map[string]*Stream
// }

// func (sm *StreamManager) AddStream(stream_id string, target_id string) {
// 	sm.streams[stream_id] = target_id
// }

// func (sm *StreamManager) RemoveStream(stream_id string) {
// 	delete(sm.streams, stream_id)
// }

// func (sm *StreamManager) FindStream(stream_id string) (*Stream, err) {
// 	sm.RLock()
// 	defer sm.RUnlock()
// 	streamPtr, ok := sm.streams[stream_id]
// 	if ok {
// 		return streamPtr, nil
// 	} else {
// 		return nil, errors.New("Bad Stream Id")
// 	}
// }

type Manager struct {
	targetsLock sync.RWMutex       //
	tokensLock  sync.RWMutex       //
	streamsLock sync.RWMutex       //
	targets     map[string]*Target // map of targetId to Target
	tokens      map[string]*Stream // map of token to Stream
	streams     map[string]*Stream // map of streamId to Stream
}

func NewManager() *Manager {
	tm := Manager{
		targets: make(map[string]*Target),
		tokens:  make(map[string]*Stream),
		streams: make(map[string]*Stream),
	}
	return &tm
}

func (m *Manager) AddToken(token string, stream *Stream) {
	m.tokensLock.Lock()
	defer m.tokensLock.Unlock()
	m.tokens[token] = stream
}

func (m *Manager) RemoveToken(token string) {
	m.tokensLock.Lock()
	defer m.tokensLock.Unlock()
	delete(m.tokens, token)
}

func (m *Manager) FindStreamFromToken(token string) *Stream {
	m.tokensLock.RLock()
	defer m.tokensLock.RUnlock()
	return m.tokens[token]
}

func (m *Manager) AddStream(streamId string, stream *Stream) {
	m.streamsLock.Lock()
	defer m.streamsLock.Unlock()
	m.streams[streamId] = stream
}

func (m *Manager) RemoveStream(streamId string) {
	m.streamsLock.Lock()
	defer m.streamsLock.Unlock()
	delete(m.streams, streamId)
}

func (m *Manager) FindStreamFromStreamId(streamId string) *Stream {
	m.streamsLock.RLock()
	defer m.streamsLock.RUnlock()
	return m.streams[streamId]
}

// Create a Target if it does not exist. Does nothing if the target exists.
func (m *Manager) CreateTarget(target_id string) {
	m.targetsLock.Lock()
	defer m.targetsLock.Unlock()
	if _, ok := m.targets[target_id]; ok == false {
		m.targets[target_id] = NewTarget(m)
	}
}

// Remove target from the manager. Does nothing if the target does not exist.
func (m *Manager) RemoveTarget(target_id string) {
	m.targetsLock.Lock()
	defer m.Unlock()
	if _, ok := m.targets[target_id]; ok == false {
		return
	}
	target := m.targets[target_id]
	target.Die()
	delete(m.targets, target_id)
	m.targetsLock.Unlock()
}

func (m *Manager) GetTarget(target_id string) *Target {
	m.targetsLock.RLock()
	defer m.targetsLock.RUnlock()
	target := m.targets[target_id]
	return target
}
