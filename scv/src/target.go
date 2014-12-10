package scv

type Target struct {
	activeStreams   map[*Stream]struct{} // set of active streams
	disabledStreams map[*Stream]struct{} // set of streams not eligible to be assigned
	inactiveStreams *Set                 // queue of inactive streams
}

func StreamComp(l, r interface{}) bool {
	s1 := l.(*Stream)
	s2 := r.(*Stream)
	if s1.Frames == s2.Frames {
		return s1.StreamId > s2.StreamId
	} else {
		return s1.Frames > s2.Frames
	}
}

func NewTarget() *Target {
	target := Target{
		// tokens:          make(map[string]*Stream),
		activeStreams:   make(map[*Stream]struct{}),
		inactiveStreams: NewCustomSet(StreamComp),
		disabledStreams: make(map[*Stream]struct{}),
		// timers:          make(map[string]*time.Timer),
	}
	return &target
}
