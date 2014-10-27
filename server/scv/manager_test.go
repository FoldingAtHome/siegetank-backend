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

var mockFunc = func(*Stream) error { return nil }

type mockInterface struct{}

func (m *mockInterface) RemoveStreamService(s *Stream) error {
	return nil
}

func (m *mockInterface) DeactivateStreamService(s *Stream) error {
	return nil
}

var intf = &mockInterface{}

func TestAddRemoveStream(t *testing.T) {
	m := NewManager(intf)
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
			m.AddStream(stream, targetId, mockFunc)
		}()
	}
	wg.Wait()
	for k, _ := range streamPtrs {
		assert.True(t, m.targets[targetId].inactiveStreams.Contains(k))
		assert.Equal(t, m.streams[k.streamId], k)
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
}

func TestRemoveActiveStream(t *testing.T) {
	m := NewManager(intf)
	targetId := util.RandSeq(5)
	streamId := util.RandSeq(5)
	stream := NewStream(streamId, targetId, "OK", 5, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, mockFunc)
	_, err := m.ActivateStream(targetId, "yutong", "openmm")
	assert.True(t, err == nil)
	assert.Equal(t, len(m.tokens), 1)
	assert.Equal(t, len(m.streams), 1)
	m.RemoveStream(streamId)
	_, ok := m.targets[targetId]
	assert.False(t, ok)
	assert.Equal(t, len(m.tokens), 0)
	assert.Equal(t, len(m.streams), 0)
}

func TestDeactivateTimer(t *testing.T) {
	m := NewManager(intf)
	targetId := util.RandSeq(5)
	streamId := util.RandSeq(5)
	stream := NewStream(streamId, targetId, "OK", 5, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, mockFunc)
	sleepTime := 5
	m.targets[targetId].ExpirationTime = sleepTime
	token, err := m.ActivateStream(targetId, "yutong", "openmm")
	assert.True(t, err == nil)
	stream.RLock()
	assert.True(t, stream.activeStream != nil)
	stream.RUnlock()
	time.Sleep(time.Duration(sleepTime) * time.Second)
	stream.RLock()
	assert.True(t, stream.activeStream == nil)
	stream.RUnlock()
	_, ok := m.timers[token]
	assert.False(t, ok)
}

func TestActivateStream(t *testing.T) {
	m := NewManager(intf)
	numStreams := 5
	targetId := util.RandSeq(5)
	addOrder := make([]*Stream, 0)
	for i := 0; i < numStreams; i++ {
		streamId := util.RandSeq(3)
		stream := NewStream(streamId, targetId, "OK", i, 0, int(time.Now().Unix()))
		m.AddStream(stream, targetId, mockFunc)
		addOrder = append(addOrder, stream)
	}
	var mu sync.Mutex
	var wg sync.WaitGroup
	activationTokens := make([]string, 0)
	// we need to make sure that the activation order is correct.
	for i := 0; i < numStreams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			// activate a single stream
			username := util.RandSeq(5)
			engine := util.RandSeq(5)
			token, err := m.ActivateStream(targetId, username, engine)
			assert.True(t, err == nil)
			mu.Lock()
			activationTokens = append(activationTokens, token)
			mu.Unlock()
			m.RLock()
			stream := m.tokens[token]
			assert.Equal(t, stream.activeStream.user, username)
			assert.Equal(t, stream.activeStream.engine, engine)
			assert.Equal(t, stream.activeStream.authToken, token)
			assert.True(t, stream.activeStream.startTime-int(time.Now().Unix()) < 2)
			m.RUnlock()
		}()
	}
	wg.Wait()
	for idx, token := range activationTokens {
		s := m.tokens[token]
		assert.Equal(t, s, addOrder[numStreams-idx-1])
	}
	for _, stream := range addOrder {
		wg.Add(1)
		go func(streamId string) {
			defer wg.Done()
			err := m.DeactivateStream(streamId)
			assert.Equal(t, err, nil)
		}(stream.streamId)
	}
	wg.Wait()
	assert.Equal(t, len(m.tokens), 0)
	assert.Equal(t, len(m.targets[targetId].activeStreams), 0)
	assert.Equal(t, m.targets[targetId].inactiveStreams.Len(), numStreams)
}

func TestStreamReadWrite(t *testing.T) {
	m := NewManager(intf)
	targetId := util.RandSeq(5)
	streamId := util.RandSeq(5)
	stream := NewStream(streamId, targetId, "OK", 0, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, mockFunc)
	_, err := m.ActivateStream(targetId, "yutong", "openmm")
	assert.True(t, err == nil)
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		if i%100 == 0 {
			go func() {
				fn := func(s *Stream) error {
					s.frames += 1
					return nil
				}
				m.ModifyStream(streamId, fn)
				wg.Done()
			}()
		} else {
			go func() {
				var frame_count int
				fn := func(s *Stream) error {
					frame_count = s.frames
					return nil
				}
				m.ReadStream(streamId, fn)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	assert.Equal(t, m.streams[streamId].frames, 10)
}

func TestActivateEmptyTarget(t *testing.T) {
	m := NewManager(intf)
	targetId := util.RandSeq(5)
	numStreams := 3
	for i := 0; i < numStreams; i++ {
		streamId := util.RandSeq(3)
		stream := NewStream(streamId, targetId, "OK", 0, 0, int(time.Now().Unix()))
		m.AddStream(stream, targetId, mockFunc)
		_, err := m.ActivateStream(targetId, "foo", "bar")
		assert.True(t, err == nil)
	}
	_, err := m.ActivateStream(targetId, "foo", "bar")
	assert.True(t, err != nil)
}

func MultiplexTest(nStreams, nActivations, nDeactivations, nResets, nModifications, nReads int) {
	streamList := make(map[string]struct{})
	
	// add asynchronously
	for t := 0; t < nTargets; t++ {
		go
		targetId := util.RandSeq(20)
		for s := 0; s < nStreams; s++ {
			streamId := util.RandSeq(3)
			stream := NewStream(streamId, targetId, "OK", 0, 0, int(time.Now().Unix()))
			m.AddStream(stream, targetId, mockFunc)

		}
		// hehehehe
	}
}

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
