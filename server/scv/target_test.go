package scv

import (
    //"time"
    "testing"
    "sync"
    "sort"
    "fmt"
    "time"
)

func TestAddRemoveStream(t *testing.T) {
    tm := NewTargetManager()
    target := NewTarget(tm)

    // uuid := randSeq(36)
    // target.AddStream(uuid)
    // target.RemoveStream(uuid)
    // target.Die()

    var wg sync.WaitGroup

    stream_indices := make([]string, 0)

    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            uuid := randSeq(36)
            stream_indices = append(stream_indices, uuid)
            target.AddStream(uuid)
        }()
    }
    wg.Wait()

    time.Sleep(time.Second)

    myMap, _ := target.GetActiveStreams()
    keys := make([]string, 0, len(myMap))
    for k := range myMap {
        keys = append(keys, k)
    }

    sort.Sort(sort.StringSlice(stream_indices))
    sort.Sort(sort.StringSlice(keys))

    fmt.Println(keys, stream_indices)

    for _, stream_id := range stream_indices {
        wg.Add(1)
        go func() {
            defer wg.Done()
            target.RemoveStream(stream_id)
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