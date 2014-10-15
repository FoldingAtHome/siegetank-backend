package scv

import (
	// "fmt"
	"errors"
	"math/rand"
	"sync"
	"time"
)

func RandSeq(n int) string {
	b := make([]rune, n)
	var letters = []rune("012345689ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

type ActiveStream struct {
    sync.RWMutex
	total_frames  float32
	buffer_frames int
	auth_token    string
	user          string
	start_time    int
	frame_hash    string
	engine        string
}

type Target struct {
	sync.RWMutex
    inactive_streams map[string]struct{}
    active_streams   map[string]*ActiveStream
	timers           map[string]*time.Timer // map of timers
	expirations      chan string            // expiration channel for the timers
	expiration_time  int                    // expiration time in seconds
    targetManager    *TargetManager
	dead_wg          sync.WaitGroup
	is_dead          bool
}

func NewTarget(tm *TargetManager) *Target {
	target := Target{
		active_streams:   make(map[string]*ActiveStream),
		inactive_streams: make(map[string]struct{}),
		timers:           make(map[string]*time.Timer),
		expirations:      make(chan string),
        expiration_time:  600,
		targetManager:    tm,
	}
	target.dead_wg.Add(1)
	go func() {
		target.run()
		target.dead_wg.Done()
	}()
	return &target
}

// This method does nothing if the target is already dead.
func (t *Target) Die() {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		return
	}
	// expire all streams too.
	close(t.expirations)
	t.dead_wg.Wait()
}

func (t *Target) AddStream(stream_id string) error {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		return errors.New("target is dead")
	}
	t.inactive_streams[stream_id] = struct{}{}
	return nil
}

func (t *Target) RemoveStream(stream_id string) error {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		return errors.New("target is dead")
	}
	delete(t.inactive_streams, stream_id)
	return nil
}

func (t *Target) ActivateStream(user, engine string) (token, stream_id string, err error) {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		return "", "", errors.New("target is dead")
	}
	for stream_id = range t.inactive_streams {
		break
	}
	token = RandSeq(5)
	as := &ActiveStream{
		user:       user,
		engine:     engine,
		auth_token: token,
		// timer: nil,
	}
	t.timers[stream_id] = time.AfterFunc(time.Second*time.Duration(t.expiration_time), func() {
		t.expirations <- stream_id
	})
	t.active_streams[stream_id] = as
	t.targetManager.Tokens.AddToken(token, as)
	delete(t.inactive_streams, stream_id)
	return
}

func (t *Target) DeactivateStream(stream_id string) error {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		return errors.New("target is dead")
	}
	t.deactivate(stream_id)
	return nil
}

func (t *Target) GetActiveStream(stream_id string) (as *ActiveStream, err error) {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		err = errors.New("target is dead")
		return
	}
	as, ok := t.active_streams[stream_id]
	if ok == false {
		err = errors.New(stream_id + "is not active")
	}
	return
}

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe. If the caller would like to
// iterate over the list of streams returned in here, he'd need to call Lock
func (t *Target) GetActiveStreams() (map[string]struct{}, error) {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		return nil, errors.New("target is dead")
	}
	copy := make(map[string]struct{})
	for k := range t.active_streams {
		copy[k] = struct{}{}
	}
	return copy, nil
}

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe.
func (t *Target) GetInactiveStreams() (map[string]struct{}, error) {
	t.Lock()
	defer t.Unlock()
	if t.is_dead {
		return nil, errors.New("target is dead")
	}
	copy := make(map[string]struct{})
	for k, v := range t.inactive_streams {
		copy[k] = v
	}
	return copy, nil
}

func (t *Target) run() {
	for {
		select {
		case stream_id, ok := <-t.expirations:
			if ok {
				t.Lock()
                t.deactivate(stream_id)
				t.Unlock()
			} else {
				return
			}
		}
	}
}

func (target *Target) deactivate(stream_id string) {
	prop, ok := target.active_streams[stream_id]
	if ok {
		target.targetManager.Tokens.RemoveToken(prop.auth_token)
		delete(target.active_streams, stream_id)
		target.inactive_streams[stream_id] = struct{}{}
	}
}
