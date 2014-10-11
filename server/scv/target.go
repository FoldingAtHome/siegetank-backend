package scv

import (
    "fmt"
    "math/rand"
    "sync"
    "time"
    "errors"
)

func randSeq(n int) string {
    b := make([]rune, n)
    var letters = []rune("012345689ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
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
    active_streams map[string]ActiveStream
    inactive_streams map[string]struct{}
    expirations chan string
    targetManager *TargetManager
    dead_wg sync.WaitGroup
    is_dead bool
}

func NewTarget(tm *TargetManager) *Target {
    target := Target{
        active_streams: make(map[string]ActiveStream),
        inactive_streams: make(map[string]struct{}),
        expirations: make(chan string),
        targetManager: tm,
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
    token = randSeq(36)
    as := ActiveStream{
        user: user, 
        engine: engine, 
        auth_token: token,
        timer: time.AfterFunc(time.Second*5, func() {
            t.expirations <- stream_id
        }),
    }
    t.active_streams[stream_id] = as
    t.targetManager.Tokens.AddToken(token, &as)
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

func (t *Target) GetActiveStream(stream_id string) (*ActiveStream, error) {
    t.Lock()
    defer t.Unlock()
    if t.is_dead {
        return nil, errors.New("target is dead")
    }
    data, err_code := t.active_streams[stream_id]
    if err_code {
        return nil, errors.New(stream_id + "is not active")
    } else {
        return &data, nil
    }
}

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe.
func (t *Target) GetActiveStreams() (map[string]*ActiveStream, error) {
    t.Lock()
    defer t.Unlock()
    if t.is_dead {
        return nil, errors.New("target is dead")
    }
    copy := make(map[string]*ActiveStream)
    for k, v := range t.active_streams {
        copy[k] = &v
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
        case stream_id, ok := <- t.expirations:
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
    fmt.Println("Deactivating", stream_id)
    prop, ok := target.active_streams[stream_id]
    if ok {
        target.targetManager.Tokens.RemoveToken(prop.auth_token)
        delete(target.active_streams, stream_id)
        target.inactive_streams[stream_id] = struct{}{}
    }
}