package scv

// Cached object persisted in Mongo
type Stream struct {
	CommandQueue
	streamId     string
	targetId     string
	owner        string
	status       string
	frames       int
	errorCount   int
	creationDate int
	activeStream *ActiveStream
}

func NewStream(streamId, targetId, owner, status string,
	frames, errorCount, creationDate int) *Stream {
	stream := &Stream{
		CommandQueue: CommandQueue{},
		streamId:     streamId,
		targetId:     targetId,
		owner:        owner,
		status:       status,
		frames:       frames,
		errorCount:   errorCount,
		creationDate: creationDate,
	}
	return s
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

func (stream *Stream) Activate(user, token, engine string) {
	stream.Dispatch(func() {
		stream.activeStream = NewActiveStream(user, token, engine)
	})
}

func (stream *Stream) Deactivate() {
	stream.Dispatch(func() {
		stream.activeStream.Die()
		stream.activeStream = nil
	})
}

// A dispatch that is guarded against active stream failures.
func (stream *Stream) DispatchAS(fn func()) (err error) {
	stream.Lock()
	defer stream.Unlock()
	if activeStream == nil {
		return errors.New("Stream is dead")
	}
	if stream.Finished {
		return errors.New("No longer accepting new commands")
	}
	fn
	return
}

func (stream *Stream) CoreFrame() (err error) {
	stream.DispatchAS(func() {

	})
}

func (stream *Stream) LoadCheckpointFiles(streamDir string) (files map[string]string, err error) {
	stream.DispatchAS(func() {
		if stream.frames > 0 {
			frameDir := filepath.Join(streamDir, strconv.Itoa(stream.frames))
			checkpointDirs, err := ioutil.ReadDir(frameDir)
			if err != nil {
				return
			}
			// find the folder containing the last checkpoint
			lastCheckpoint := 0
			for _, fileProp := range checkpointDirs {
				count, _ := strconv.Atoi(fileProp.Name())
				if count > lastCheckpoint {
					lastCheckpoint = count
				}
			}
			checkpointDir := filepath.Join(frameDir, "checkpoint_files")
			checkpointFiles, err := ioutil.ReadDir(checkpointDir)
			if err != nil {
				return
			}
			for _, fileProp := range checkpointFiles {
				binary, err := ioutil.ReadFile(filepath.Join(checkpointDir, fileProp.Name()))
				if err != nil {
					return
				}
				files[fileProp.Name()] = string(binary)
			}
		}
		seedDir := filepath.Join(streamDir, "files")
		seedFiles, err := ioutil.ReadDir(seedDir)
		if err != nil {
			return
		}
		for _, fileProp := range seedFiles {
			// insert seedFile only if it's not already included from checkpoint
			_, ok := files[fileProp.Name()]
			if ok == false {
				binary, err := ioutil.ReadFile(filepath.Join(seedDir, fileProp.Name()))
				if err != nil {
					return
				}
				files[fileProp.Name()] = string(binary)
			}
		}
	})
	return
}
