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
	"strconv"
	"sync"
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
	stats      *list.List
	statsWG    sync.WaitGroup
	statsMutex sync.Mutex
	shutdown   chan os.Signal
	finish     chan struct{}
}

type Stats struct {
	Engine    string  `bson:"engine"`
	User      string  `bson:"user"`
	StartTime int     `bson:"start_time"`
	EndTime   int     `bson:"end_time"`
	Frames    float64 `bson:"frames"`
	Stream    string  `bson:"stream"`
	targetId  string  `bson:"-"`
}

func (app *Application) DeactivateStreamService(s *Stream) error {
	stat := Stats{
		Engine:    s.activeStream.engine,
		User:      s.activeStream.user,
		StartTime: s.activeStream.startTime,
		EndTime:   int(time.Now().Unix()),
		Frames:    s.activeStream.donorFrames,
		Stream:    s.StreamId,
		targetId:  s.TargetId,
	}
	app.statsMutex.Lock()
	app.stats.PushBack(interface{}(stat))
	app.statsMutex.Unlock()
	return nil
}

func (app *Application) drainStats() {
	app.statsMutex.Lock()
	for app.stats.Len() > 0 {
		ele := app.stats.Front()
		s := ele.Value.(Stats)
		cursor := app.Mongo.DB("stats").C(s.targetId)
		err := cursor.Insert(s)
		if err == nil {
			app.stats.Remove(ele)
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	app.statsMutex.Unlock()
}

func (app *Application) RecordStatsService() {
	defer app.statsWG.Done()
	for {
		select {
		case <-app.finish:
			app.drainStats()
			return
		default:
			app.drainStats()
			time.Sleep(1 * time.Second)
		}
	}
}

// Add SSL stuff later
type Configuration struct {
	MongoURI     string
	Name         string
	Password     string
	ExternalHost string
	InternalHost string
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
	app.Manager = NewManager(&app)
	app.Router = mux.NewRouter()
	app.Router.Handle("/streams", app.StreamsHandler()).Methods("POST")
	app.Router.Handle("/streams/info/{stream_id}", app.StreamInfoHandler()).Methods("GET")
	app.Router.Handle("/streams/activate", app.StreamActivateHandler()).Methods("POST")
	app.Router.Handle("/streams/download/{stream_id}/{file:.+}", app.StreamDownloadHandler()).Methods("GET")
	app.Router.Handle("/streams/start/{stream_id}", app.StreamEnableHandler()).Methods("PUT")
	app.Router.Handle("/streams/stop/{stream_id}", app.StreamDisableHandler()).Methods("PUT")
	app.Router.Handle("/core/start", app.CoreStartHandler()).Methods("GET")
	app.Router.Handle("/core/frame", app.CoreFrameHandler()).Methods("POST")
	app.Router.Handle("/core/checkpoint", app.CoreCheckpointHandler()).Methods("POST")
	app.Router.Handle("/core/stop", app.CoreStopHandler()).Methods("PUT")
	app.server = NewServer("127.0.0.1:12345", app.Router)
	app.statsWG.Add(1)
	return &app
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
	go func() {
		err := app.server.ListenAndServe()
		if err != nil {
			log.Println("ListenAndServe: ", err)
		}
	}()
	go app.RecordStatsService()
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
	app.Shutdown()
}

func (app *Application) Shutdown() {
	// Order matters here!
	app.server.Close()
	close(app.finish)
	app.statsWG.Wait()
	app.Mongo.Close()
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
			if err != nil {
				return errors.New("Unable to find stream's owner.")
			}
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
			// TODO: update frame count in MongoDB
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

func (app *Application) StreamEnableHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		user, auth_err := app.CurrentManager(r)
		if auth_err != nil {
			return auth_err
		}
		streamId := mux.Vars(r)["stream_id"]
		e := app.Manager.EnableStream(streamId, user)
		if e != nil {
			return e
		}
		cursor := app.Mongo.DB("streams").C(app.Config.Name)
		return cursor.UpdateId(streamId, bson.M{"status": "OK"})
	}
}

func (app *Application) StreamDisableHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) error {
		user, auth_err := app.CurrentManager(r)
		if auth_err != nil {
			return auth_err
		}
		streamId := mux.Vars(r)["stream_id"]
		e := app.Manager.DisableStream(streamId, user)
		if e != nil {
			return e
		}
		cursor := app.Mongo.DB("streams").C(app.Config.Name)
		return cursor.UpdateId(streamId, bson.M{"status": "STOPPED"})
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
		cursor := app.Mongo.DB("streams").C(app.Config.Name)
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
		//cursor := app.Mongo.DB("streams").C(app.Config.Name)
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
