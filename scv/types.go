package scv

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// map of tokens to active streams
type TokenManager struct {
	sync.RWMutex
	tokens map[string]*ActiveStream
}

type ActiveStream struct {
	total_frames  float32
	buffer_frames int
	auth_token    string
	user          string
	start_time    int
	frame_hash    string
	engine        string
	timer         *time.Timer
}

type Target struct {
	sync.RWMutex
	active_streams   map[string]ActiveStream
	inactive_streams map[string]bool
	expirations      chan string
	targetManager    *TargetManager
}

type TargetManager struct {
	sync.RWMutex
	targets      map[string]Target
	tokenManager TokenManager
}

func randSeq(n int) string {
	b := make([]rune, n)
	var letters = []rune("012345689ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
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

func (tm *TargetManager) AddTarget(target_id string) {
	tm.Lock()
	tm.targets[target_id] = Target{
		active_streams:   make(map[string]ActiveStream),
		inactive_streams: make(map[string]bool),
		expirations:      make(chan string),
		targetManager:    tm,
	}
	tm.Unlock()
}

func (tm *TargetManager) RemoveTarget(target_id string) {
	tm.Lock()
	delete(tm.targets, target_id)
	tm.Unlock()
}

func (tm *TargetManager) FindTarget(target_id string) *Target {
	tm.RLock()
	target := tm.targets[target_id]
	tm.RUnlock()
	return &target
}

func (target *Target) AddStream(stream_id string) {
	target.Lock()
	target.inactive_streams[stream_id] = true
	target.Unlock()
}

func (target *Target) RemoveStream(stream_id string) {
	target.Lock()
	target.unguardedDeactivateStream(stream_id)
	delete(target.inactive_streams, stream_id)
	target.Unlock()
}

func (target *Target) ActivateStream(user, engine string) string {
	target.Lock()
	var stream_id string
	// pick an inactivate stream randomly
	// TODO: replace with queue
	for stream_id = range target.inactive_streams {
		break
	}
	if len(stream_id) > 0 {
		token := randSeq(36)
		prop := ActiveStream{
			// start_frame:   0,
			total_frames:  0,
			buffer_frames: 0,
			auth_token:    token,
			user:          user,
			start_time:    int(time.Now().Unix()),
			engine:        engine,
			timer: time.AfterFunc(time.Duration(600)*time.Second, func() {
				target.expirations <- stream_id
			}),
		}
		target.active_streams[stream_id] = prop
		target.targetManager.tokenManager.AddToken(token, &prop)
		delete(target.inactive_streams, stream_id)
	}
	target.Unlock()
	return stream_id
}

func (target *Target) unguardedDeactivateStream(stream_id string) {
	prop, ok := target.active_streams[stream_id]
	if ok {
		fmt.Println("Deactivating stream", stream_id)
		target.targetManager.tokenManager.RemoveToken(prop.auth_token)
		delete(target.active_streams, stream_id)
		target.inactive_streams[stream_id] = true
	}
}

func (target *Target) DeactivateStream(stream_id string) {
	target.Lock()
	target.unguardedDeactivateStream(stream_id)
	target.Unlock()
}

func (target *Target) Init() {
	for {
		select {
		case stream_id, ok := <- target.expirations:
			if ok {
				target.DeactivateStream(stream_id)
			} else {
				return
			}
		}
	}
}

func (target *Target) Cleanup() {
	close(target.expirations)
}
