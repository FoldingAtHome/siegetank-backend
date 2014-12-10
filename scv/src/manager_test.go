package scv

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var _ = fmt.Printf

var mockFunc = func(*Stream) error { return nil }

type mockInterface struct{}

func (m *mockInterface) DeactivateStreamService(s *Stream) error {
	return nil
}

func (m *mockInterface) DisableStreamService(s *Stream) error {
	return nil
}

func (m *mockInterface) EnableStreamService(s *Stream) error {
	return nil
}

var intf = &mockInterface{}

func TestAddSameStream(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(36)
	streamId := RandSeq(36)
	stream := NewStream(streamId, targetId, "none", 0, 0, int(time.Now().Unix()))
	err := m.AddStream(stream, targetId, true)
	assert.Nil(t, err)
	err = m.AddStream(stream, targetId, true)
	assert.NotNil(t, err)
}

func TestStreamError(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "none", 5, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)

	for i := 0; i < MAX_STREAM_FAILS; i++ {
		token, _, err := m.ActivateStream(targetId, "yutong", "openmm", mockFunc)
		assert.Nil(t, err)
		err = m.DeactivateStream(token, 1)
		assert.Nil(t, err)
	}
	_, _, err := m.ActivateStream(targetId, "yutong", "openmm", mockFunc)
	assert.NotNil(t, err)
}

func TestStreamNoError(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "none", 5, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)

	for i := 0; i < MAX_STREAM_FAILS; i++ {
		token, _, err := m.ActivateStream(targetId, "yutong", "openmm", mockFunc)
		assert.Nil(t, err)
		err = m.DeactivateStream(token, 0)
		assert.Nil(t, err)
	}
	_, _, err := m.ActivateStream(targetId, "yutong", "openmm", mockFunc)
	assert.Nil(t, err)
}

func TestAddRemoveStream(t *testing.T) {
	m := NewManager(intf)
	var wg sync.WaitGroup
	var mutex sync.Mutex
	streamPtrs := make(map[*Stream]struct{})
	targetId := RandSeq(36)
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			streamId := RandSeq(36)
			stream := NewStream(streamId, targetId, "none", 0, 0, int(time.Now().Unix()))
			mutex.Lock()
			streamPtrs[stream] = struct{}{}
			mutex.Unlock()
			m.AddStream(stream, targetId, true)
		}()
	}
	wg.Wait()
	for k, _ := range streamPtrs {
		assert.True(t, m.targets[targetId].inactiveStreams.Contains(k))
		assert.Equal(t, m.streams[k.StreamId], k)
	}
	for k, _ := range streamPtrs {
		wg.Add(1)
		go func(stream_id string) {
			defer wg.Done()
			m.RemoveStream(stream_id, "none")
		}(k.StreamId)
	}
	wg.Wait()
	_, ok := m.targets[targetId]
	assert.False(t, ok)
	for k, _ := range streamPtrs {
		_, ok := m.streams[k.StreamId]
		assert.False(t, ok)
	}
}

func TestRemoveDisabledStream(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "none", 5, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)
	assert.Nil(t, m.DisableStream(streamId, "none"))
	assert.Nil(t, m.RemoveStream(streamId, "none"))
	_, ok := m.targets[targetId]
	assert.False(t, ok)
}

func TestRemoveActiveStream(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "none", 5, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)
	_, _, err := m.ActivateStream(targetId, "yutong", "openmm", mockFunc)
	assert.Nil(t, err)
	assert.Equal(t, len(m.tokens), 1)
	assert.Equal(t, len(m.streams), 1)
	m.RemoveStream(streamId, "none")
	_, ok := m.targets[targetId]
	assert.False(t, ok)
	assert.Equal(t, len(m.streams), 0)
}

func TestDeactivateTimer(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "none", 0, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)
	sleepTime := 6
	m.expirationTime = 5
	_, streamId, err := m.ActivateStream(targetId, "yutong", "openmm", mockFunc)
	assert.True(t, err == nil)
	m.ReadStream(streamId, func(s *Stream) error {
		assert.NotNil(t, s.activeStream)
		return nil
	})
	time.Sleep(time.Duration(sleepTime) * time.Second)
	m.ReadStream(streamId, func(s *Stream) error {
		assert.Nil(t, s.activeStream)
		return nil
	})

	assert.Nil(t, stream.activeStream)
}

func TestReadModifyStream(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "none", 0, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)
	err := m.ModifyActiveStream("bad_token", mockFunc)
	assert.NotNil(t, err)
	err = m.ModifyActiveStream("bad_token:asdf", mockFunc)
	assert.NotNil(t, err)
	err = m.ReadStream("bad_stream", mockFunc)
	assert.NotNil(t, err)
	err = m.ModifyStream("bad_stream", mockFunc)
	assert.NotNil(t, err)
	err = m.RemoveStream("bad_stream", "none")
	assert.NotNil(t, err)
	err = m.ReadStream(streamId, mockFunc)
	assert.Nil(t, err)
	err = m.ModifyStream(streamId, mockFunc)
	assert.Nil(t, err)
	m.RemoveStream(streamId, "none")
}

func TestEnableDisableStream(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "some_user", 0, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)
	assert.NotNil(t, m.DisableStream(streamId, "some_bad_user"))
	assert.Nil(t, m.DisableStream(streamId, "some_user"))
	assert.Nil(t, m.DisableStream(streamId, "some_user"))
	username := RandSeq(5)
	engine := RandSeq(5)
	_, _, err := m.ActivateStream(targetId, username, engine, mockFunc)
	assert.NotNil(t, err)
	assert.NotNil(t, m.EnableStream(streamId, "some_bad_user"))
	assert.Nil(t, m.EnableStream(streamId, "some_user"))
	assert.Nil(t, m.EnableStream(streamId, "some_user"))
	token, _, err := m.ActivateStream(targetId, username, engine, mockFunc)
	assert.Nil(t, m.ModifyActiveStream(token, mockFunc))
	assert.Nil(t, err)
	assert.NotNil(t, m.DisableStream(streamId, "some_bad_user"))
	assert.Nil(t, m.DisableStream(streamId, "some_user"))
	assert.Nil(t, m.DisableStream(streamId, "some_user"))
	_, _, err = m.ActivateStream(targetId, username, engine, mockFunc)
	assert.NotNil(t, err)
	assert.NotNil(t, m.ModifyActiveStream("bad_token", mockFunc))

	assert.NotNil(t, m.EnableStream("bad_streams", "some_user"))
}

func TestActivateStream(t *testing.T) {
	m := NewManager(intf)
	numStreams := 5
	targetId := RandSeq(5)
	addOrder := make([]*Stream, 0)
	for i := 0; i < numStreams; i++ {
		streamId := RandSeq(3)
		stream := NewStream(streamId, targetId, "none", i, 0, int(time.Now().Unix()))
		m.AddStream(stream, targetId, true)
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
			username := RandSeq(5)
			engine := RandSeq(5)
			token, _, err := m.ActivateStream(targetId, username, engine, mockFunc)
			assert.Nil(t, err)
			mu.Lock()
			activationTokens = append(activationTokens, token)
			mu.Unlock()
			m.RLock()
			// target := m.targets[targetId]
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
		go func(token string) {
			defer wg.Done()
			err := m.DeactivateStream(token, 0)
			assert.Nil(t, err)
		}(stream.activeStream.authToken)
	}
	wg.Wait()
	assert.Equal(t, len(m.tokens), 0)
	assert.Equal(t, len(m.targets[targetId].activeStreams), 0)
	assert.Equal(t, m.targets[targetId].inactiveStreams.Len(), numStreams)
}

func TestStreamReadWrite(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	streamId := RandSeq(5)
	stream := NewStream(streamId, targetId, "none", 0, 0, int(time.Now().Unix()))
	m.AddStream(stream, targetId, true)
	_, _, err := m.ActivateStream(targetId, "yutong", "openmm", mockFunc)
	assert.Nil(t, err)
	var wg sync.WaitGroup
	for i := 0; i < 1000; i++ {
		wg.Add(1)
		if i%100 == 0 {
			go func() {
				fn := func(s *Stream) error {
					s.Frames += 1
					return nil
				}
				m.ModifyStream(streamId, fn)
				wg.Done()
			}()
		} else {
			go func() {
				var frame_count int
				fn := func(s *Stream) error {
					frame_count = s.Frames
					return nil
				}
				m.ReadStream(streamId, fn)
				wg.Done()
			}()
		}
	}
	wg.Wait()
	assert.Equal(t, m.streams[streamId].Frames, 10)
}

func TestActivateEmptyTarget(t *testing.T) {
	m := NewManager(intf)
	targetId := RandSeq(5)
	numStreams := 3
	for i := 0; i < numStreams; i++ {
		streamId := RandSeq(3)
		stream := NewStream(streamId, targetId, "none", 0, 0, int(time.Now().Unix()))
		m.AddStream(stream, targetId, true)
		_, _, err := m.ActivateStream(targetId, "foo", "bar", mockFunc)
		assert.Nil(t, err)
	}
	_, _, err := m.ActivateStream(targetId, "foo", "bar", mockFunc)
	assert.NotNil(t, err)
}

type MultiplexTester struct {
	t *testing.T
}

type testStats struct {
	sync.Mutex
	count float64
	sum   float64
}

func (s *testStats) Add(val float64) {
	s.Lock()
	s.count += 1.0
	s.sum += val
	s.Unlock()
}

func (s *testStats) Mean() float64 {
	s.Lock()
	defer s.Unlock()
	return s.sum / s.count
}

func (mt *MultiplexTester) Multiplex(nTargets, nStreams, nActivations, secondsBetweenFrames int) error {
	// add asynchronously
	m := NewManager(intf)
	// this test ends after 5 minutes
	m.expirationTime = 300
	testDurationInMilliseconds := int64(900)
	modifyStreamStats := testStats{}
	activateStreamStats := testStats{}
	var wg sync.WaitGroup
	for t := 0; t < nTargets; t++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			targetId := RandSeq(20)
			var wg2 sync.WaitGroup
			for s := 0; s < nStreams; s++ {
				wg2.Add(1)
				go func() {
					defer wg2.Done()
					// add streams at random points in time
					streamId := RandSeq(12)
					stream := NewStream(streamId, targetId, "none", 0, 0, int(time.Now().Unix()))
					err := m.AddStream(stream, targetId, true)
					assert.Nil(mt.t, err)
				}()
			}
			wg2.Wait()
			// activate streams at random points in time (some of these may fail, which is fine)
			for a := 0; a < nActivations; a++ {
				// bad XX intrinsically racy as hell
				wg.Add(1)
				go func() {
					defer wg.Done()
					// activate these streams over the span of 1 minutes
					time.Sleep(time.Second * time.Duration(rand.Intn(secondsBetweenFrames)))
					k := time.Now().UnixNano()
					token, _, err := m.ActivateStream(targetId, "joe", "bob", mockFunc)
					if err == nil {
						activateDuration := float64(time.Now().UnixNano() - k)
						// fmt.Println("Activate Duration", activateDuration/float64(1e6))
						activateStreamStats.Add(activateDuration)
						wg.Add(1)
						go func() {
							defer wg.Done()
							for {
								writeFileMock := func(s *Stream) error {
									// fmt.Println(time.Now().Unix(), "doing file io... stream:", streamId, "target:", targetId)
									time.Sleep(time.Duration(testDurationInMilliseconds) * time.Millisecond)
									return nil
								}
								s := time.Now().UnixNano()
								err := m.ModifyActiveStream(token, writeFileMock)
								if err != nil {
									break
								} else {
									modifyDuration := float64(time.Now().UnixNano() - s)
									// fmt.Println("Modify Duration", modifyDuration/float64(1e6))
									modifyStreamStats.Add(modifyDuration)
								}
								time.Sleep(time.Second * time.Duration(secondsBetweenFrames))
							}
						}()
					}
				}()
			}
		}()
	}
	wg.Wait()
	assert.True(mt.t, activateStreamStats.Mean()/float64(1e6) < 10)
	assert.True(mt.t, modifyStreamStats.Mean()/float64(1e6)-float64(testDurationInMilliseconds) < 0.01*float64(testDurationInMilliseconds))

	return nil
}

func TestMultiplex(t *testing.T) {
	mt := MultiplexTester{t}

	// 50 targets, 20000 streams per target, 2000 active streams per target (activated over a span of 1 hour)
	// mt.Multiplex(50, 20000, 2000, 300)
	mt.Multiplex(10, 100, 100, 20)
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
// 			stream_id := RandSeq(3)
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
