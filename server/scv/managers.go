package scv

import(
    "sync"
)

// map of tokens to active streams
type TokenManager struct {
    sync.RWMutex
    tokens map[string]*ActiveStream
}

func (t *TokenManager) AddToken(token string, stream *ActiveStream) {
    t.Lock()
    t.tokens[token] = stream
    t.Unlock()
}

func (t *TokenManager) RemoveToken(token string) {
    t.Lock()
    delete(t.tokens, token)
    t.Unlock()
}

func (t *TokenManager) FindStream(token string) *ActiveStream {
    t.RLock()
    stream := t.tokens[token]
    t.RUnlock()
    return stream
}

type TargetManager struct {
    sync.RWMutex
    targets      map[string]Target
    Tokens TokenManager
}

func NewTargetManager() *TargetManager {
    tm := TargetManager{
        targets: make(map[string]Target),
        Tokens: TokenManager{tokens: make(map[string]*ActiveStream)},
    }
    return &tm
}

// func (tm *TargetManager) AddTarget(target_id string) {
//     tm.Lock()
//     tm.targets[target_id] = Target{
//         active_streams:   make(map[string]ActiveStream),
//         inactive_streams: make(map[string]bool),
//         expirations:      make(chan string),
//         targetManager:    tm,
//     }
//     tm.Unlock()
// }

// func (tm *TargetManager) RemoveTarget(target_id string) {
//     tm.Lock()
//     target := tm.targets[target_id]
//     target.Cleanup()
//     delete(tm.targets, target_id)
//     tm.Unlock()
// }

// func (tm *TargetManager) FindTarget(target_id string) *Target {
//     tm.RLock()
//     target := tm.targets[target_id]
//     tm.RUnlock()
//     return &target
// }
