package scv

import (
	"errors"
	"sync"
)

// map of tokens to active streams
type TokenManager struct {
	sync.RWMutex
	tokens map[string]*ActiveStream
}

func (t *TokenManager) AddToken(token string, stream *ActiveStream) {
	t.Lock()
	defer t.Unlock()
	t.tokens[token] = stream
}

func (t *TokenManager) RemoveToken(token string) {
	t.Lock()
	defer t.Unlock()
	delete(t.tokens, token)
}

func (t *TokenManager) FindStream(token string) (*ActiveStream, error) {
	t.RLock()
	defer t.RUnlock()
	stream, ok := t.tokens[token]
	if ok {
		return stream, nil
	} else {
		return nil, errors.New("Bad Token")
	}
}

type TargetManager struct {
	sync.RWMutex
	targets map[string]*Target
	Tokens  TokenManager
}

func NewTargetManager() *TargetManager {
	tm := TargetManager{
		targets: make(map[string]*Target),
		Tokens:  TokenManager{tokens: make(map[string]*ActiveStream)},
	}
	return &tm
}

// Add stream to a given target. If the target does not exist, then it is created.
func (tm *TargetManager) AddStreamToTarget(target_id string, stream_id string, frames int) {
	tm.Lock()
	if _, ok := tm.targets[target_id]; ok == false {
		tm.targets[target_id] = NewTarget(tm)
	}
	tm.targets[target_id].AddStream(stream_id, frames)
	tm.Unlock()
}

// Remove target from the manager. Does nothing if the target does not exist
func (tm *TargetManager) RemoveTarget(target_id string) {
	tm.Lock()
	defer tm.Unlock()
	if _, ok := tm.targets[target_id]; ok == false {
		return
	}
	target := tm.targets[target_id]
	target.Die()
	delete(tm.targets, target_id)
	tm.Unlock()
}

func (tm *TargetManager) GetTarget(target_id string) *Target {
	tm.RLock()
	defer tm.RUnlock()
	target := tm.targets[target_id]
	return target
}
