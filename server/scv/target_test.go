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
            uuid := RandSeq(36)
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
		    uuid := RandSeq(3)
		    target.AddStream(uuid)
        }()
	}
	var wg sync.WaitGroup
	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// activate a single stream
			username := RandSeq(5)
			engine := RandSeq(5)
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
            stream_id := RandSeq(3)
            target.AddStream(stream_id)
            token, stream_id, err := target.ActivateStream("foo", "bar")
            assert.Equal(t, stream_id, stream_id)
            assert.True(t, err == nil)
            _, err = target.GetActiveStream(stream_id)
            assert.True(t, err == nil)
            _, err = tm.Tokens.FindStream(token)
            assert.True(t, err == nil)
            inactive_streams, err := target.GetInactiveStreams()
            _, ok := inactive_streams[stream_id]
            assert.False(t, ok)
            time.Sleep(time.Duration(target.expiration_time+1)*time.Second)
            _, err = target.GetActiveStream(stream_id)
            assert.True(t, err != nil)
            _, err = tm.Tokens.FindStream(token)
            assert.True(t, err != nil)
            inactive_streams, err = target.GetInactiveStreams()
            _, ok = inactive_streams[stream_id]
            assert.True(t, ok)
        }()
        time.Sleep(2*time.Second)
    }
    wg.Wait()
    target.Die()
}
