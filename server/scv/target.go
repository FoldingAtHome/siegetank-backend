package scv

import (
	"../util"
	"container/heap"
	"errors"
	"fmt"
	// "sort"
	"strconv"
	"time"
)

var _ = fmt.Printf

type ActiveStream struct {
	CommandQueue
	entireFrames int     // number of completed frames (does not include partial frames)
	donorFrames  float64 // number of frames done by this donor (including partial frames)
	bufferFrames int     // number of frames stored in the buffer
	authToken    string  // token of the ActiveStream
	user         string  // donor id
	startTime    int     // time the stream was activated
	frameHash    string  // md5 hash of the last frame
	engine       string  // core engine type the stream is assigned to
}

func NewActiveStream(user, token, engine string, entireFrames int) *ActiveStream {
	as := &ActiveStream{
		CommandQueue: makeCommandQueue(),
		user:         user,
		engine:       engine,
		authToken:    token,
		entireFrames: entireFrames,
		startTime:    int(time.Now().Unix()),
	}
	go as.run()
	return as
}

func (as *ActiveStream) run() {
	for {
		select {
		case msg := <-as.commands:
			msg.fn()
			msg.wait <- struct{}{}
		case <-as.finished:
			return
		}
	}
}

func (as *ActiveStream) LoadCheckpointFiles() (files map[string]string, err error) {
	as.Dispatch(func() {
		if as.entireFrames > 0 {
			frameDir := filepath.join(app.StreamDir(stream_id), str(frames))
			checkpointDirs, err := ioutil.ReadDir(frameDir)
			if err != nil {
				return
			}
			// find the folder containing the last checkpoint
			lastCheckpoint := 0
			for _, fileProp := range checkpointDirs {
				count := strconv.Atoi(fileProp.Name())
				if count > lastcheckpoint {
					lastCheckpoint = count
				}
			}
			checkpointFiles, err := ioutil.ReadDir(filepath.join(frameDir, "checkpoint_files"))
			if err != nil {
				return
			}
			for _, fileProp := range checkpointFiles {
				files[fileProp.Name()] = ioutil.ReadFile(filepath.join(checkpointFiles, fileProp.Name()))
			}
		}
		seedDir := filepath.join(app.StreamDir(stream_id), "files")
		seedFiles, err := ioutil.ReadDir(seedDir)
		if err != nil {
			return
		}
		for _, fileProp := range seedFiles {
			_, ok := files[fileProp.Name()]
			if ok == false {
				files[fileProp.Name()] = ioutil.ReadFile(filepath.join(seedDir, fileProp.Name()))
			}
		}
	})
	return
}

type Target struct {
	CommandQueue
	inactiveStreams PriorityQueue
	activeStreams   map[string]*ActiveStream
	timers          map[string]*time.Timer // map of timers
	expirations     chan string            // expiration channel for the timers
	ExpirationTime  int                    // expiration time in seconds
	targetManager   *TargetManager
}

func NewTarget(tm *TargetManager) *Target {
	target := Target{
		CommandQueue:    makeCommandQueue(),
		inactiveStreams: make(PriorityQueue, 0),
		activeStreams:   make(map[string]*ActiveStream),
		timers:          make(map[string]*time.Timer),
		expirations:     make(chan string),
		ExpirationTime:  600,
		targetManager:   tm,
	}
	heap.Init(&target.inactiveStreams)
	go target.run()
	return &target
}

func (t *Target) AddStream(stream_id string, weight int) error {
	return t.Dispatch(func() {
		item := &QueueItem{
			value:    stream_id,
			priority: weight,
		}
		heap.Push(&t.inactiveStreams, item)
	})
}

func (t *Target) RemoveStream(stream_id string) error {
	return t.Dispatch(func() {
		t.deactivate(stream_id)
		heap.Pop(&t.inactiveStreams)
	})
}

func (t *Target) ActivateStream(user, engine string) (token, stream_id string, err error) {
	err2 := t.Dispatch(func() {
		var entireFrames int
		for _, qItem := range t.inactiveStreams {
			stream_id = qItem.value
			entireFrames = qItem.priority
			break
		}
		if stream_id == "" {
			err = errors.New("No streams can be activated.")
			return
		}
		token = util.RandSeq(36)
		as := NewActiveStream(user, token, engine, entireFrames)
		t.timers[stream_id] = time.AfterFunc(time.Second*time.Duration(t.ExpirationTime), func() {
			t.expirations <- stream_id
		})
		t.activeStreams[stream_id] = as
		t.targetManager.Tokens.AddToken(token, as)
		heap.Pop(&t.inactiveStreams)
	})
	if err2 != nil {
		err = err2
		return
	}
	if err != nil {
		return
	}
	return
}

func (t *Target) DeactivateStream(stream_id string) error {
	return t.Dispatch(func() {
		t.deactivate(stream_id)
	})
}

func (t *Target) ActiveStream(stream_id string) (as *ActiveStream, err error) {
	var ok bool
	err2 := t.Dispatch(func() {
		as, ok = t.activeStreams[stream_id]
	})
	if err2 != nil {
		return nil, err2
	}
	if ok == false {
		return nil, errors.New("Stream is not active")
	}
	return
}

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe. If the caller would like to
// iterate over the list of streams returned in here, he'd need to call Lock
func (t *Target) ActiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for k := range t.activeStreams {
			copy[k] = struct{}{}
		}
	})
	return
}

// Returns a copy. Adding to this map will not affect the actual map used by
// the target since maps are not goroutine safe.
func (t *Target) InactiveStreams() (copy map[string]struct{}, err error) {
	copy = make(map[string]struct{})
	err = t.Dispatch(func() {
		for _, item := range t.inactiveStreams {
			copy[item.value] = struct{}{}
		}
	})
	return copy, err
}

func (t *Target) run() {
	for {
		select {
		case stream_id := <-t.expirations:
			t.deactivate(stream_id)
		case msg := <-t.commands:
			msg.fn()
			msg.wait <- struct{}{}
		case <-t.finished:
			return
		}
	}
}

func (target *Target) deactivate(stream_id string) {
	activeStream, ok := target.activeStreams[stream_id]
	if ok {
		target.targetManager.Tokens.RemoveToken(activeStream.authToken)
		item := &QueueItem{
			value:    stream_id,
			priority: target.activeStreams[stream_id].entireFrames,
		}
		activeStream.Die()
		delete(target.activeStreams, stream_id)
		heap.Push(&target.inactiveStreams, item)
		delete(target.timers, stream_id)
	}
}
