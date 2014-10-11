package scv

import (
	//"time"
	"sync"
	"testing"
	// "sort"
	// "fmt"
	// "time"

	"github.com/stretchr/testify/assert"
)

// func TestAddRemoveStream(t *testing.T) {
//     tm := NewTargetManager()
//     target := NewTarget(tm)
//     var wg sync.WaitGroup
//     stream_indices := make(map[string]struct{})
//     for i := 0; i < 10; i++ {
//         wg.Add(1)
//         go func() {
//             defer wg.Done()
//             uuid := randSeq(36)
//             // NOT SAFE
//             stream_indices[uuid] = struct{}{}
//             target.AddStream(uuid)
//         }()
//     }
//     wg.Wait()
//     time.Sleep(time.Second)
//     myMap, _ := target.GetInactiveStreams()
//     assert.Equal(t, myMap, stream_indices)

//     for key, _ := range stream_indices {
//         wg.Add(1)
//         go func() {
//             defer wg.Done()
//             target.RemoveStream(key)
//         }()
//     }
//     wg.Wait()

//     target.Die()
// }

func TestActivateStream(t *testing.T) {
	tm := NewTargetManager()
	target := NewTarget(tm)
	var wg sync.WaitGroup
	stream_indices := make(map[string]struct{})
	numStreams := 100
	for i := 0; i < numStreams; i++ {
		// wg.Add(1)
		// go func() {
		// defer wg.Done()
		uuid := randSeq(3)
		stream_indices[uuid] = struct{}{}
		target.AddStream(uuid)
		// }()
	}

	wg.Wait()
	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// activate a single stream
			username := randSeq(5)
			engine := randSeq(5)
			_, stream_id, err := target.ActivateStream(username, engine)
			if err != nil {
				t.Errorf("Failed to activate a stream")
			}
			as, err := target.GetActiveStream(stream_id)
			assert.Equal(t, as.user, username)
			assert.Equal(t, as.engine, engine)
			active_streams, err := target.GetActiveStreams()
			if err != nil {
				t.Errorf("Unable to retrieve active streams")
			}
			_, ok := active_streams[stream_id]

			assert.True(t, ok)
		}()
	}
	wg.Wait()
	target.Die()
}
