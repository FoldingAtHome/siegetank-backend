package scv

import (
	//"time"
	"../util"
	// "sort"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var _ = fmt.Printf

func TestAddRemoveStream(t *testing.T) {
	m := NewManager()
	var wg sync.WaitGroup
	var mutex sync.Mutex
	streamPtrs := make(map[*Stream]struct{})
	targetId := util.RandSeq(36)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			streamId := util.RandSeq(36)
			stream := NewStream(streamId, targetId, "OK", 0, 0, int(time.Now().Unix()))
			mutex.Lock()
			streamPtrs[stream] = struct{}{}
			mutex.Unlock()
			m.AddStream(stream, targetId)
		}()
	}
	wg.Wait()
	for k, _ := range streamPtrs {
		assert.True(t, m.targets[targetId].inactiveStreams.Contains(k))
		_, ok := m.streams[k.streamId]
		assert.True(t, ok)
	}
	for k, _ := range streamPtrs {
		wg.Add(1)
		go func(stream_id string) {
			defer wg.Done()
			m.RemoveStream(stream_id)
		}(k.streamId)
	}
	wg.Wait()
	_, ok := m.targets[targetId]
	assert.False(t, ok)
	for k, _ := range streamPtrs {
		_, ok := m.streams[k.streamId]
		assert.False(t, ok)
	}
	// removed, _ := target.InactiveStreams()
	// assert.Equal(t, removed, make(map[string]struct{}))
	// target.Die()
}

// func TestDie(t *testing.T) {
// 	target := NewTarget(NewTargetManager())
// 	_, err := target.ActiveStreams()
// 	assert.True(t, err == nil)
// 	target.Die()
// 	_, err = target.ActiveStreams()
// 	assert.True(t, err != nil)
// }

// func TestActivateStream(t *testing.T) {
// 	tm := NewTargetManager()
// 	target := NewTarget(tm)
// 	numStreams := 5
// 	add_order := make([]string, 0)
// 	for i := 0; i < numStreams; i++ {
// 		uuid := util.RandSeq(3)
// 		target.AddStream(uuid, i)
// 		add_order = append(add_order, uuid)
// 	}
// 	var mu sync.Mutex
// 	var wg sync.WaitGroup
// 	activation_order := make([]string, 0)
// 	// we need to make sure that the activation order is correct.
// 	for i := 0; i < numStreams; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			// activate a single stream
// 			username := util.RandSeq(5)
// 			engine := util.RandSeq(5)
// 			token, stream_id, err := target.ActivateStream(username, engine)
// 			mu.Lock()
// 			activation_order = append(activation_order, stream_id)
// 			mu.Unlock()
// 			assert.True(t, err == nil)
// 			as, err := target.ActiveStream(stream_id)
// 			assert.Equal(t, as.user, username)
// 			assert.Equal(t, as.engine, engine)
// 			assert.Equal(t, as.authToken, token)
// 			assert.True(t, as.startTime-int(time.Now().Unix()) < 2)
// 			active_streams, err := target.ActiveStreams()
// 			assert.True(t, err == nil)
// 			_, ok := active_streams[stream_id]
// 			token_stream, err := tm.Tokens.FindStream(token)
// 			assert.True(t, err == nil)
// 			assert.Equal(t, as, token_stream)
// 			assert.True(t, ok)
// 		}()
// 	}
// 	wg.Wait()
// 	// activation_order should be equivalent to the reversed add_order
// 	for i, j := 0, len(add_order)-1; i < j; i, j = i+1, j-1 {
// 		add_order[i], add_order[j] = add_order[j], add_order[i]
// 	}
// 	assert.Equal(t, add_order, activation_order)
// 	// deactivate the highest stream and make sure the priority is carried over
// 	best_stream := activation_order[0]
// 	target.DeactivateStream(best_stream)
// 	cop, _ := target.InactiveStreams()
// 	_, ok := cop[best_stream]
// 	assert.True(t, ok)
// 	cop, _ = target.ActiveStreams()
// 	_, ok = cop[best_stream]
// 	assert.False(t, ok)
// 	assert.Equal(t, target.inactiveStreams[0].priority, numStreams-1)
// 	target.Die()
// }

// func TestEmptyActivation(t *testing.T) {
// 	tm := NewTargetManager()
// 	target := NewTarget(tm)
// 	numStreams := 3
// 	for i := 0; i < numStreams; i++ {
// 		stream_id := util.RandSeq(3)
// 		target.AddStream(stream_id, 0)
// 		_, _, err := target.ActivateStream("foo", "bar")
// 		assert.True(t, err == nil)
// 	}
// 	_, _, err := target.ActivateStream("foo", "bar")
// 	assert.True(t, err != nil)
// }

// func TestStreamExpiration(t *testing.T) {
// 	tm := NewTargetManager()
// 	target := NewTarget(tm)
// 	target.ExpirationTime = 7
// 	numStreams := 3
// 	// add three streams in intervals of three seconds
// 	var wg sync.WaitGroup
// 	for i := 0; i < numStreams; i++ {
// 		wg.Add(1)
// 		go func() {
// 			defer wg.Done()
// 			stream_id := util.RandSeq(3)
// 			target.AddStream(stream_id, 0)
// 			token, stream_id, err := target.ActivateStream("foo", "bar")
// 			assert.Equal(t, stream_id, stream_id)
// 			assert.True(t, err == nil)
// 			_, err = target.ActiveStream(stream_id)
// 			assert.True(t, err == nil)
// 			_, err = tm.Tokens.FindStream(token)
// 			assert.True(t, err == nil)
// 			inactive_streams, err := target.InactiveStreams()
// 			_, ok := inactive_streams[stream_id]
// 			assert.False(t, ok)
// 			time.Sleep(time.Duration(target.ExpirationTime+1) * time.Second)
// 			_, err = target.ActiveStream(stream_id)
// 			assert.True(t, err != nil)
// 			_, err = tm.Tokens.FindStream(token)
// 			assert.True(t, err != nil)
// 			inactive_streams, err = target.InactiveStreams()
// 			_, ok = inactive_streams[stream_id]
// 			assert.True(t, ok)
// 		}()
// 		time.Sleep(2 * time.Second)
// 	}
// 	wg.Wait()
// 	target.Die()
// }
