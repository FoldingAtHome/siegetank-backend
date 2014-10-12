package scv

import (
	//"time"
	"sync"
	"testing"
	// "sort"
	"time"
    // "fmt"

	"github.com/stretchr/testify/assert"
)

func TestAddRemoveStream(t *testing.T) {
    tm := NewTargetManager()
    target := NewTarget(tm)
    var wg sync.WaitGroup
    var mutex sync.Mutex
    stream_indices := make(map[string]struct{})
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            uuid := randSeq(36)
            mutex.Lock()
            stream_indices[uuid] = struct{}{}
            mutex.Unlock()
            target.AddStream(uuid)
        }()
    }
    wg.Wait()
    myMap, _ := target.GetInactiveStreams()
    assert.Equal(t, myMap, stream_indices)

    for key, _ := range stream_indices {
        wg.Add(1)
        go func(stream_id string) {
            defer wg.Done()
            target.RemoveStream(stream_id)
        }(key)
    }
    wg.Wait()
    removed, _ := target.GetInactiveStreams()
    assert.Equal(t, removed, make(map[string]struct{}))

    target.Die()
}

func TestActivateStream(t *testing.T) {
	tm := NewTargetManager()
	target := NewTarget(tm)
	numStreams := 5
	for i := 0; i < numStreams; i++ {
        go func() {
		    uuid := randSeq(3)
		    target.AddStream(uuid)
        }()
	}
	var wg sync.WaitGroup
	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// activate a single stream
			username := randSeq(5)
			engine := randSeq(5)
			token, stream_id, err := target.ActivateStream(username, engine)
			assert.True(t, err == nil)
			as, err := target.GetActiveStream(stream_id)
			assert.Equal(t, as.user, username)
			assert.Equal(t, as.engine, engine)
            assert.Equal(t, as.auth_token, token)
			active_streams, err := target.GetActiveStreams()
			assert.True(t, err == nil)
			_, ok := active_streams[stream_id]
            token_stream, err := tm.Tokens.FindStream(token)
            assert.True(t, err == nil)
            assert.Equal(t, as, token_stream)
			assert.True(t, ok)
		}()
	}
	wg.Wait()
	target.Die()
}

func TestStreamExpiration(t *testing.T) {
    tm := NewTargetManager()
    target := NewTarget(tm)
    target.expiration_time = 7
    numStreams := 3
    // add three streams in intervals of three seconds
    var wg sync.WaitGroup
    for i := 0; i < numStreams; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            uuid := randSeq(3)
            target.AddStream(uuid)
            _, stream_id, err := target.ActivateStream("foo", "bar")
            assert.Equal(t, stream_id, uuid)
            assert.True(t, err == nil)
            _, err = target.GetActiveStream(uuid)
            assert.True(t, err == nil)
            time.Sleep(time.Duration(target.expiration_time)*time.Second)
            _, err = target.GetActiveStream(uuid)
            assert.False(t, err != nil)
        }()
        time.Sleep(2*time.Second)
    }
    wg.Wait()
}
