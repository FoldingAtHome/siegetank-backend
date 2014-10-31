package scv

import (
	"sync"
	"time"
)

// Cached object persisted in Mongo
type Stream struct {
	sync.RWMutex `json:"-" bson:"-"`
	StreamId     string `json:"-" bson:"_id"`
	TargetId     string `json:"target_id" bson:"target_id"`
	Status       string `json:"stats" bson:"status"`
	Frames       int    `json:"frames" bson:"frames"`
	ErrorCount   int    `json:"error_count" bson:"error_count"`
	CreationDate int    `json:"creation_date" bson:"creation_date"`
	activeStream *ActiveStream
}

func NewStream(streamId, targetId, status string,
	frames, errorCount, creationDate int) *Stream {
	stream := &Stream{
		StreamId:     streamId,
		TargetId:     targetId,
		Status:       status,
		Frames:       frames,
		ErrorCount:   errorCount,
		CreationDate: creationDate,
	}
	return stream
}

type ActiveStream struct {
	donorFrames  float64 // number of frames done by this donor (including partial frames)
	bufferFrames int     // number of frames stored in the buffer
	authToken    string  // token of the ActiveStream
	user         string  // donor id
	startTime    int     // time the stream was activated
	frameHash    string  // md5 hash of the last frame
	engine       string  // core engine type the stream is assigned to
}

func NewActiveStream(user, token, engine string) *ActiveStream {
	as := &ActiveStream{
		user:      user,
		engine:    engine,
		authToken: token,
		startTime: int(time.Now().Unix()),
	}
	return as
}
