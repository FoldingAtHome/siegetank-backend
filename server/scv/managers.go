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

// // Does nothing if the target already exists
// func (tm *TargetManager) AddTarget(target_id string) {
//     tm.Lock()
//     if _, ok := tm.target[target_id]; ok {
//         return
//     }
//     tm.targets[target_id] = &Target{
//         activeStreams:   make(map[string]ActiveStream),
//         inactiveStreams: make(map[string]bool),
//         expirations:      make(chan string),
//         targetManager:    tm,
//     }
//     tm.Unlock()
// }

// // Remove target from the manager. Does nothing if the target does not exist
// func (tm *TargetManager) RemoveTarget(target_id string) {
//     tm.Lock()
//     defer tm.Unlock()
//     if _, ok := tm.target[target_id]; ok == false {
//         return
//     }
//     target := tm.targets[target_id]
//     target.Die()
//     delete(tm.targets, target_id)
//     tm.Unlock()
// }

// func (tm *TargetManager) GetTarget(target_id string) *Target {
//     tm.RLock()
//     defer tm.RUnlock()
//     target := tm.targets[target_id]
//     return &target
// }
