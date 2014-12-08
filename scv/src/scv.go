package scv

import (
	"bytes"
	"compress/gzip"
	"container/list"
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"../util"
)

var _ = fmt.Printf

type Application struct {
	Config  Configuration
	Mongo   *mgo.Session
	Manager *Manager
	Router  *mux.Router

	server     *Server
	stats      *list.List // things we put in this list should persist when server dies
	statsWG    sync.WaitGroup
	statsMutex sync.Mutex
	shutdown   chan os.Signal
	finish     chan struct{}
}

/*
Record statistics for the Stream s. There is no need to manually called DisableStreamService to
set the status to "disabled" if error_count exceeds the limit.

Usually when this method is called, it's in a double mutex.
*/
func (app *Application) DeactivateStreamService(s *Stream) error {
	// Record stats for stream and defer insertion until later.
	stats := make(map[string]interface{})
	streamId := s.StreamId
	donorFrames := s.activeStream.donorFrames
	stats["engine"] = s.activeStream.engine
	stats["user"] = s.activeStream.user
	stats["start_time"] = s.activeStream.startTime
	stats["end_time"] = int(time.Now().Unix())
	stats["frames"] = donorFrames
	stats["stream"] = streamId
	stats_cursor := app.Mongo.DB("stats").C(s.TargetId)
	fn1 := func() error {
		return stats_cursor.Insert(stats)
	}
	status := "enabled"
	if s.ErrorCount >= MAX_STREAM_FAILS {
		status = "disabled"
	}
	// Update frames, error_count, and status in Mongo
	stream_prop := bson.M{"$set": bson.M{"frames": s.Frames, "error_count": s.ErrorCount, "status": status}}
	stream_cursor := app.Mongo.DB("streams").C(app.Config.Name)
	fn2 := func() error {
		// Possible corner case where the stream is actually deleted here.
		stream_cursor.UpdateId(streamId, stream_prop)
		// We do not care about the return code.
		return nil
	}

	app.statsMutex.Lock()
	if donorFrames > 0 {
		app.stats.PushBack(fn1)
	}
	app.stats.PushBack(fn2)
	app.statsMutex.Unlock()
	return nil
}

func (app *Application) EnableStreamService(s *Stream) error {
	cursor := app.Mongo.DB("streams").C(app.Config.Name)
	s.ErrorCount = 0
	s.MongoStatus = "enabled"
	return cursor.UpdateId(s.StreamId, bson.M{"$set": bson.M{"status": "enabled", "error_count": 0}})
}

func (app *Application) DisableStreamService(s *Stream) error {
	cursor := app.Mongo.DB("streams").C(app.Config.Name)
	// fmt.Println("DISABLING STREAM", streamId)
	return cursor.UpdateId(s.StreamId, bson.M{"$set": bson.M{"status": "disabled"}})
}

func (app *Application) drainStats() {
	app.statsMutex.Lock()
	for app.stats.Len() > 0 {
		ele := app.stats.Front()
		fn := ele.Value.(func() error)
		err := fn()
		if err == nil {
			app.stats.Remove(ele)
		} else {
			fmt.Println(err)
			break
		}
	}
	app.statsMutex.Unlock()
}

func (app *Application) RecordDeferredDocs() {
	defer app.statsWG.Done()
	for {
		select {
		case <-app.finish:
			app.drainStats()

			// persist stats here if not empty
			// if app.stats.Len() > 0 {
			// }

			return
		default:
			app.drainStats()
			time.Sleep(1 * time.Second)
		}
	}
}

type Configuration struct {
	MongoURI     string            `json:"MongoURI" bson:"-"`
	Name         string            `json:"Name" bson:"_id"`
	Password     string            `json:"Password" bson:"password"`
	ExternalHost string            `json:"ExternalHost" bson:"host"`
	InternalHost string            `json:"InternalHost" bson:"-"`
	SSL          map[string]string `json:"SSL" bson:"-"`
}

func (app *Application) RegisterSCV() {
	log.Printf("Registering SCV %s with database...", app.Config.Name)
	cursor := app.Mongo.DB("servers").C("scvs")
	_, err := cursor.UpsertId(app.Config.Name, app.Config)
	if err != nil {
		panic("Could not connect to MongoDB: " + err.Error())
	}
}

func (app *Application) LoadStreams() {
	var mongoStreams []Stream

	err := app.StreamsCursor().Find(bson.M{}).All(&mongoStreams)
	if err != nil {
		panic("Could not connect to MongoDB: " + err.Error())
	}

	mongoStreamIds := make(map[string]Stream)
	for _, val := range mongoStreams {
		mongoStreamIds[val.StreamId] = val
	}

	log.Printf("Loading %d streams...", len(mongoStreamIds))

	diskStreamIds := make(map[string]struct{})
	fileData, err := ioutil.ReadDir(filepath.Join(app.Config.Name+"_data", "streams"))
	for _, v := range fileData {
		diskStreamIds[v.Name()] = struct{}{}
	}
	// Check that disk streams is equal to mongo streams. That is mongoStreams /subset of diskStreamIds
	for streamId, stream := range mongoStreamIds {
		_, ok := diskStreamIds[streamId]
		if ok == false {
			log.Panicln("Cannot find data for stream " + streamId + " on disk")
		}
		partitions, err := app.ListPartitions(streamId)
		if err != nil {
			panic("Unable to list partitions for stream " + streamId)
		}
		lastFrame := 0
		if len(partitions) > 0 {
			lastFrame = partitions[len(partitions)-1]
		}
		if lastFrame != stream.Frames {
			log.Printf("Warning: frame count mismatch for stream %s. Disk: %d, Mongo: %d, using disk value.", streamId, lastFrame, stream.Frames)
		}
		stream.Frames = lastFrame
		mongoStreamIds[streamId] = stream
	}
	for streamId, _ := range diskStreamIds {
		_, ok := mongoStreamIds[streamId]
		if ok == false {
			streamDir := app.StreamDir(streamId)
			log.Println("Warning: stream " + streamId + " is present on disk but not in Mongo, removing " + streamDir)
			os.RemoveAll(streamDir)
		}
	}

	for _, stream := range mongoStreamIds {
		if stream.MongoStatus == "enabled" {
			app.Manager.AddStream(&stream, stream.TargetId, true)
		} else if stream.MongoStatus == "disabled" {
			app.Manager.AddStream(&stream, stream.TargetId, false)
		} else {
			panic("Unknown stream status")
		}
	}
}

func NewApplication(config Configuration) *Application {
	session, err := mgo.Dial(config.MongoURI)
	if err != nil {
		panic(err)
	}
	app := Application{
		Config:  config,
		Mongo:   session,
		Manager: nil,
		stats:   list.New(),
		finish:  make(chan struct{}),
	}

	index := mgo.Index{
		Key:        []string{"target_id"},
		Background: true, // See notes.
	}
	app.StreamsCursor().EnsureIndex(index)

	app.Manager = NewManager(&app)
	app.Router = mux.NewRouter()
	app.Router.Handle("/", app.AliveHandler()).Methods("GET")
	app.Router.Handle("/streams", app.StreamsHandler()).Methods("POST")
	app.Router.Handle("/streams/info/{stream_id}", app.StreamInfoHandler()).Methods("GET")
	app.Router.Handle("/streams/activate", app.StreamActivateHandler()).Methods("POST")
	app.Router.Handle("/streams/download/{stream_id}/{file:.+}", app.StreamDownloadHandler()).Methods("GET")
	app.Router.Handle("/streams/start/{stream_id}", app.StreamEnableHandler()).Methods("PUT")
	app.Router.Handle("/streams/stop/{stream_id}", app.StreamDisableHandler()).Methods("PUT")
	app.Router.Handle("/streams/delete/{stream_id}", app.StreamDeleteHandler()).Methods("PUT")
	app.Router.Handle("/streams/sync/{stream_id}", app.StreamSyncHandler()).Methods("GET")
	app.Router.Handle("/core/start", app.CoreStartHandler()).Methods("GET")
	app.Router.Handle("/core/frame", app.CoreFrameHandler()).Methods("POST")
	app.Router.Handle("/core/checkpoint", app.CoreCheckpointHandler()).Methods("POST")
	app.Router.Handle("/core/stop", app.CoreStopHandler()).Methods("PUT")
	app.Router.Handle("/core/heartbeat", app.CoreHeartbeatHandler()).Methods("POST")
	app.server = NewServer(config.InternalHost, app.Router)
	if len(config.SSL) > 0 {
		app.server.TLS(config.SSL["Cert"], config.SSL["Key"])
		app.server.CA(config.SSL["CA"])
	}
	app.statsWG.Add(1)
	return &app
}

func (app *Application) StreamsCursor() *mgo.Collection {
	return app.Mongo.DB("streams").C(app.Config.Name)
}

type AppHandler func(http.ResponseWriter, *http.Request) error

// if we need to return different error codes, simply subclass error class and implement a different type of error
// and use a dynamic switch type?
func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := fn(w, r); err != nil {
		http.Error(w, err.Error(), 400)
	}
}

// Look up the User using the Authorization header
func (app *Application) CurrentUser(r *http.Request) (user string, err error) {
	token := r.Header.Get("Authorization")
	cursor := app.Mongo.DB("users").C("all")
	result := make(map[string]interface{})
	if err = cursor.Find(bson.M{"token": token}).One(&result); err != nil {
		return
	}
	user = result["_id"].(string)
	return
}

func (app *Application) IsManager(user string) bool {
	cursor := app.Mongo.DB("users").C("managers")
	result := make(map[string]interface{})
	if err := cursor.Find(bson.M{"_id": user}).One(&result); err != nil {
		return false
	} else {
		return true
	}
}

func (app *Application) CurrentManager(r *http.Request) (user string, err error) {
	user, err = app.CurrentUser(r)
	if err != nil {
		return "", errors.New("Unable to find user.")
	}
	isManager := app.IsManager(user)
	if isManager == false {
		return "", errors.New("Not a manager.")
	}
	return user, nil
}

// Return a path indicating where stream files should be stored
func (app *Application) StreamDir(stream_id string) string {
	return filepath.Join(app.Config.Name+"_data", "streams", stream_id)
}

// Run starts the server. Listens and Serves asynchronously. And sets up necessary
// signal handlers for graceful termination. This blocks until a signal is sent
func (app *Application) Run() {
	log.Printf("Starting up server (pid: %d) on %s", os.Getpid(), app.Config.InternalHost)
	// log.Printf("Internal host: %s, external host: %s", app.Config.InternalHost, app.Config.ExternalHost)
	app.RegisterSCV()
	app.LoadStreams()
	go func() {
		log.Println("Success! Now serving requests...")
		err := app.server.ListenAndServe()
		if err != nil {
			log.Println("ListenAndServe: ", err)
		}
	}()
	go app.RecordDeferredDocs()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill, syscall.SIGTERM)
	<-c
	app.Shutdown()
}

func (app *Application) Shutdown() {
	log.Printf("Shutting down gracefully...")
	app.server.Close()
	close(app.finish)
	app.statsWG.Wait()
	app.Mongo.Close()
}

func (app *Application) AliveHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		return nil
	}
}

func (app *Application) StreamActivateHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		if r.Header.Get("Authorization") != app.Config.Password {
			return errors.New("Unauthorized")
		}
		type Message struct {
			TargetId string `json:"target_id"`
			Engine   string `json:"engine"`
			User     string `json:"user"`
		}
		msg := Message{}
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&msg)
		if err != nil {
			return errors.New("Bad request: " + err.Error())
		}
		fn := func(s *Stream) error {
			err := os.RemoveAll(filepath.Join(app.StreamDir(s.StreamId), "buffer_files"))
			return err
		}
		token, _, err := app.Manager.ActivateStream(msg.TargetId, msg.User, msg.Engine, fn)
		if err != nil {
			return errors.New("Unable to activate stream: " + err.Error())
		}
		type Reply struct {
			token string
		}
		data, _ := json.Marshal(map[string]string{"token": token})
		w.Write(data)
		return
	}
}

func splitExt(path string) (root string, ext string) {
	ext = filepath.Ext(path)
	root = path[0 : len(path)-len(ext)]
	return
}

func maxCheckpoint(path string) (int, error) {
	checkpointDirs, e := ioutil.ReadDir(path)
	if e != nil {
		return 0, errors.New("Cannot read frames directory")
	}
	// find the folder containing the last checkpoint
	lastCheckpoint := 0
	for _, fileProp := range checkpointDirs {
		count, _ := strconv.Atoi(fileProp.Name())
		if count > lastCheckpoint {
			lastCheckpoint = count
		}
	}
	return lastCheckpoint, nil
}

func (app *Application) StreamDownloadHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		streamId := mux.Vars(r)["stream_id"]
		file := mux.Vars(r)["file"]
		absStreamDir, _ := filepath.Abs(filepath.Join(app.StreamDir(streamId)))
		requestedFile, _ := filepath.Abs(filepath.Join(app.StreamDir(streamId), file))
		if len(requestedFile) < len(absStreamDir) {
			return errors.New("Invalid file path")
		}
		if requestedFile[0:len(absStreamDir)] != absStreamDir {
			return errors.New("Invalid file path.")
		}
		user, err := app.CurrentUser(r)
		if err != nil {
			return errors.New("Unable to find user.")
		}
		return app.Manager.ReadStream(streamId, func(stream *Stream) error {
			if stream.Owner != user {
				return errors.New("You do not own this stream.")
			}
			binary, e := ioutil.ReadFile(requestedFile)
			if e != nil {
				return errors.New("Unable to read file.")
			}
			w.Write(binary)
			return nil
		})
	}
}

// Return the number of partitions in a stream.
func (app *Application) ListPartitions(streamId string) ([]int, error) {
	res := make([]int, 0)
	files, err := ioutil.ReadDir(app.StreamDir(streamId))
	if err != nil {
		return nil, errors.New("FATAL StreamSyncHandler(), can't read streamDir")
	}
	for _, fileInfo := range files {
		num, err2 := strconv.Atoi(fileInfo.Name())
		if err2 == nil && num > 0 {
			res = append(res, num)
		}
	}
	sort.Ints(res)
	return res, nil
}

func (app *Application) StreamSyncHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		streamId := mux.Vars(r)["stream_id"]
		user, auth_err := app.CurrentManager(r)
		if auth_err != nil {
			return auth_err
		}

		result := make(map[string]interface{})

		listSeeds := func() []string {
			seedDir := filepath.Join(app.StreamDir(streamId), "files")
			files, err := ioutil.ReadDir(seedDir)
			if err != nil {
				panic("FATAL StreamSyncHandler(), can't read seedDir" + seedDir)
			}
			res := make([]string, 0)
			for _, fileInfo := range files {
				res = append(res, fileInfo.Name())
			}
			return res
		}

		listFramesAndCheckpoints := func(min_partition int) ([]string, []string) {

			frames := make([]string, 0)
			checkpoints := make([]string, 0)

			frameDir := filepath.Join(app.StreamDir(streamId), strconv.Itoa(min_partition), "0")

			frameFiles, err := ioutil.ReadDir(frameDir)
			if err != nil {
				panic("FATAL StreamSyncHandler(), can't read frameDir: " + frameDir)
			}
			for _, fileInfo := range frameFiles {
				if fileInfo.Name() != "checkpoint_files" {
					frames = append(frames, fileInfo.Name())
				}
			}
			checkpointDir := filepath.Join(frameDir, "checkpoint_files")
			checkpointFiles, err := ioutil.ReadDir(checkpointDir)
			if err != nil {
				panic("FATAL StreamSyncHandler(), can't read checkpointDir: " + checkpointDir)
			}
			for _, fileInfo := range checkpointFiles {
				if fileInfo.Name() != "checkpoint_files" {
					checkpoints = append(checkpoints, fileInfo.Name())
				}
			}
			return frames, checkpoints
		}

		e := app.Manager.ReadStream(streamId, func(stream *Stream) error {
			if stream.Owner != user {
				return errors.New("You do not own this stream.")
			}
			partitions, err := app.ListPartitions(streamId)
			if err != nil {
				return err
			}
			result["partitions"] = partitions
			result["seed_files"] = listSeeds()
			if len(partitions) > 0 {
				result["frame_files"], result["checkpoint_files"] = listFramesAndCheckpoints(partitions[0])
			}
			return nil
		})
		if e != nil {
			return e
		}
		data, e := json.Marshal(result)
		if e != nil {
			return e
		}
		w.Write(data)
		return nil
	}
}

func (app *Application) StreamEnableHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		user, auth_err := app.CurrentManager(r)
		if auth_err != nil {
			return auth_err
		}
		streamId := mux.Vars(r)["stream_id"]
		return app.Manager.EnableStream(streamId, user)
	}
}

func (app *Application) StreamDisableHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		user, auth_err := app.CurrentManager(r)
		if auth_err != nil {
			return auth_err
		}
		streamId := mux.Vars(r)["stream_id"]
		return app.Manager.DisableStream(streamId, user)
	}
}

func (app *Application) StreamDeleteHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		streamId := mux.Vars(r)["stream_id"]
		user, auth_err := app.CurrentManager(r)
		if auth_err != nil {
			return auth_err
		}
		err := app.Manager.RemoveStream(streamId, user)
		if err != nil {
			return err
		}
		fn1 := func() error {
			return app.StreamsCursor().RemoveId(streamId)
		}
		app.statsMutex.Lock()
		app.stats.PushBack(fn1)
		app.statsMutex.Unlock()
		return nil
	}
}

func (app *Application) StreamsHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		user, auth_err := app.CurrentManager(r)
		if auth_err != nil {
			return auth_err
		}
		type Message struct {
			TargetId string            `json:"target_id"`
			Files    map[string]string `json:"files"`
			Tags     map[string]string `json:"tags,omitempty"`
		}
		msg := Message{}
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&msg)
		if err != nil {
			return errors.New("Bad request: " + err.Error())
		}
		streamId := util.RandSeq(36)
		// Add files to disk
		stream := NewStream(streamId, msg.TargetId, user, 0, 0, int(time.Now().Unix()))
		todo := map[string]map[string]string{"files": msg.Files, "tags": msg.Tags}
		for Directory, Content := range todo {
			for filename, fileb64 := range Content {
				files_dir := filepath.Join(app.StreamDir(streamId), Directory)
				os.MkdirAll(files_dir, 0776)
				err = ioutil.WriteFile(filepath.Join(files_dir, filename), []byte(fileb64), 0776)
				if err != nil {
					return err
				}
			}
		}
		cursor := app.StreamsCursor()
		err = cursor.Insert(stream)
		if err != nil {
			// clean up
			os.RemoveAll(app.StreamDir(streamId))
			return errors.New("Unable insert stream into DB")
		}
		// Insert stream into Manager after ensuring state is correct.
		e := app.Manager.AddStream(stream, msg.TargetId, true)
		if e != nil {
			return e
		}
		data, err := json.Marshal(map[string]string{"stream_id": streamId})
		if e != nil {
			return e
		}
		w.Write(data)
		return
	}
}

func (app *Application) StreamInfoHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		//cursor := app.StreamsCursor()
		//msg := mongoStream{}
		streamId := mux.Vars(r)["stream_id"]
		var result []byte
		e := app.Manager.ReadStream(streamId, func(stream *Stream) error {
			result, err = json.Marshal(stream)
			return err
		})
		if e != nil {
			return e
		}
		w.Write(result)
		return nil
	}
}

func pathExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func (app *Application) CoreFrameHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		token := r.Header.Get("Authorization")
		md5String := r.Header.Get("Content-MD5")
		body, _ := ioutil.ReadAll(r.Body)
		h := md5.New()
		io.WriteString(h, string(body))
		if md5String != hex.EncodeToString(h.Sum(nil)) {
			return errors.New("MD5 mismatch")
		}
		return app.Manager.ModifyActiveStream(token, func(stream *Stream) error {
			type Message struct {
				Files  map[string]string `json:"files"`
				Frames int               `json:"frames"`
			}
			msg := Message{Frames: 1}
			decoder := json.NewDecoder(bytes.NewReader(body))
			err := decoder.Decode(&msg)
			if err != nil {
				return errors.New("Could not decode JSON")
			}
			if md5String == stream.activeStream.frameHash {
				return errors.New("POSTed same frame twice")
			}
			stream.activeStream.frameHash = md5String
			for filename, filestring := range msg.Files {
				root, ext := splitExt(filename)
				filebin := []byte(filestring)
				if ext == ".b64" {
					filename = root
					reader := base64.NewDecoder(base64.StdEncoding, bytes.NewReader(filebin))
					filecopy, err := ioutil.ReadAll(reader)
					if err != nil {
						return err
					}
					filebin = filecopy
					root, ext := splitExt(filename)
					if ext == ".gz" {
						filename = root
						reader, err := gzip.NewReader(bytes.NewReader(filebin))
						defer reader.Close()
						if err != nil {
							return err
						}
						filecopy, err := ioutil.ReadAll(reader)
						if err != nil {
							return err
						}
						filebin = filecopy
					}
				}
				dir := filepath.Join(app.StreamDir(stream.StreamId), "buffer_files")
				os.MkdirAll(dir, 0776)
				filename = filepath.Join(dir, filename)
				file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0776)
				defer file.Close()
				if err != nil {
					return err
				}
				_, err = file.Write(filebin)
				if err != nil {
					return err
				}
			}
			stream.activeStream.bufferFrames += 1
			return nil
		})
	}
}

func (app *Application) CoreCheckpointHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		token := r.Header.Get("Authorization")
		md5String := r.Header.Get("Content-MD5")
		body, _ := ioutil.ReadAll(r.Body)
		h := md5.New()
		io.WriteString(h, string(body))
		if md5String != hex.EncodeToString(h.Sum(nil)) {
			return errors.New("MD5 mismatch")
		}
		return app.Manager.ModifyActiveStream(token, func(stream *Stream) error {
			streamDir := app.StreamDir(stream.StreamId)
			bufferDir := filepath.Join(streamDir, "buffer_files")
			checkpointDir := filepath.Join(bufferDir, "checkpoint_files")
			os.MkdirAll(checkpointDir, 0776)
			type Message struct {
				Files  map[string]string `json:"files"`
				Frames float64           `json:"frames"`
			}
			msg := Message{}
			decoder := json.NewDecoder(bytes.NewReader(body))
			err := decoder.Decode(&msg)
			if err != nil {
				return errors.New("Could not decode JSON")
			}
			for filename, filestring := range msg.Files {
				fileDir := filepath.Join(checkpointDir, filename)
				fileBin := []byte(filestring)
				ioutil.WriteFile(fileDir, fileBin, 0776)
			}
			bufferFrames := stream.activeStream.bufferFrames
			sumFrames := stream.Frames + bufferFrames
			partition := filepath.Join(streamDir, strconv.Itoa(sumFrames))
			os.MkdirAll(partition, 0766)
			var renameDir string

			if bufferFrames == 0 {
				exist, _ := pathExists(partition)
				if exist {
					lastCheckpoint, _ := maxCheckpoint(partition)
					renameDir = filepath.Join(partition, strconv.Itoa(lastCheckpoint+1))
				} else {
					renameDir = filepath.Join(partition, "1")
				}
			} else {
				renameDir = filepath.Join(partition, "0")
			}
			os.Rename(bufferDir, renameDir)
			stream.Frames = sumFrames
			stream.activeStream.donorFrames += msg.Frames
			stream.activeStream.bufferFrames = 0
			// TODO: update frame count in MongoDB (do we want to?)
			// This stream is mutex'd
			return nil
		})
	}
}

func (app *Application) CoreStartHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		token := r.Header.Get("Authorization")
		type Reply struct {
			StreamId string            `json:"stream_id"`
			TargetId string            `json:"target_id"`
			Files    map[string]string `json:"files"`
			Options  interface{}       `json:"options"`
		}
		rep := Reply{
			Files:   make(map[string]string),
			Options: make(map[string]interface{}),
		}
		e := app.Manager.ModifyActiveStream(token, func(stream *Stream) error {
			rep.StreamId = stream.StreamId
			rep.TargetId = stream.TargetId
			// Load stream's options from Mongo
			cursor := app.Mongo.DB("data").C("targets")
			mgoRes := make(map[string]interface{})
			if err = cursor.Find(bson.M{"_id": stream.TargetId}).One(&mgoRes); err != nil {
				return errors.New("Cannot load target's options")
			}
			rep.Options = mgoRes["options"]
			// Load the streams' files
			if stream.Frames > 0 {
				frameDir := filepath.Join(app.StreamDir(rep.StreamId), strconv.Itoa(stream.Frames))
				lastCheckpoint, _ := maxCheckpoint(frameDir)
				checkpointDir := filepath.Join(frameDir, strconv.Itoa(lastCheckpoint), "checkpoint_files")
				checkpointFiles, e := ioutil.ReadDir(checkpointDir)
				if e != nil {
					return errors.New("Cannot load checkpoint directory")
				}
				for _, fileProp := range checkpointFiles {
					binary, e := ioutil.ReadFile(filepath.Join(checkpointDir, fileProp.Name()))
					if e != nil {
						return errors.New("Cannot read checkpoint file")
					}
					rep.Files[fileProp.Name()] = string(binary)
				}
			}
			seedDir := filepath.Join(app.StreamDir(rep.StreamId), "files")
			seedFiles, e := ioutil.ReadDir(seedDir)
			if e != nil {
				return errors.New("Cannot read seed directory")
			}
			for _, fileProp := range seedFiles {
				_, ok := rep.Files[fileProp.Name()]
				if ok == false {
					binary, e := ioutil.ReadFile(filepath.Join(seedDir, fileProp.Name()))
					if e != nil {
						return errors.New("Cannot read seed files")
					}
					rep.Files[fileProp.Name()] = string(binary)
				}
			}
			return nil
		})
		if e != nil {
			return e
		}
		data, e := json.Marshal(rep)
		if e != nil {
			return e
		}
		w.Write(data)
		return
	}
}

func (app *Application) CoreStopHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		token := r.Header.Get("Authorization")
		type Message struct {
			Error string `json:"error"`
		}
		msg := Message{}
		if r.Body != nil {
			decoder := json.NewDecoder(r.Body)
			err = decoder.Decode(&msg)
			if err != nil {
				return
			}
		}
		error_count := 0
		if msg.Error != "" {
			error_count += 1
		}
		return app.Manager.DeactivateStream(token, error_count)
	}
}

func (app *Application) CoreHeartbeatHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		token := r.Header.Get("Authorization")
		return app.Manager.ResetActiveStream(token)
	}
}
