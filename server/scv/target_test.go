package scv

import (
    //"time"
    "testing"
    "sync"
    // "sort"
    // "fmt"
    "time"

    "github.com/stretchr/testify/assert"
)

func TestAddRemoveStream(t *testing.T) {
    tm := NewTargetManager()
    target := NewTarget(tm)

    // uuid := randSeq(36)
    // target.AddStream(uuid)
    // target.RemoveStream(uuid)
    // target.Die()

    var wg sync.WaitGroup

    stream_indices := make(map[string]struct{})
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            uuid := randSeq(36)
            stream_indices[uuid] = struct{}{}
            target.AddStream(uuid)
        }()
    }
    wg.Wait()
    time.Sleep(time.Second)
    myMap, _ := target.GetInactiveStreams()
    assert.Equal(t, myMap, stream_indices)

    for key, _ := range stream_indices {
        wg.Add(1)
        go func() {
            defer wg.Done()
            target.RemoveStream(key)
        }()
    } 
    wg.Wait()
    
    target.Die()
}

// func TestActivateStream(t *testing.T) {
//     tm := NewTargetManager()
//     target := NewTarget(tm)
//     var wg sync.WaitGroup

//     stream_indices := make([]string, 0)

//     for i := 0; i < 10; i++ {
//         wg.Add(1)
//         go func() {
//             defer wg.Done()
//             uuid := randSeq(36)
//             stream_indices = append(stream_indices, uuid)
//             target.AddStream(uuid)
//         }()
//     }
//     wg.Wait()

//     // activate a single stream
//     stream_id, err := target.ActivateStream("foo", "bar")
//     if err != nil {
//         t.Errorf("Failed to activate a stream")
//     }

//     as := target.GetActiveStream(stream_id)
//     as2 := tm.Tokens.FindStream(as.auth_token)

//     if as != as2 {
//         t.Errorf("Token mismatch")
//     }

//     copy := target.GetActiveStreams()

//     fmt.Println(copy)

//     time.Sleep(time.Duration(100)*time.Second)

// }