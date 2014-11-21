package scv

import (
	"bytes"
	"compress/gzip"
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
	"time"

	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"../util"
)

var _ = fmt.Printf

// func DownloadHandler(w http.ResponseWriter, req *http.Request) (err error) {
// 	time.Sleep(time.Duration(10) * time.Second)
// 	io.WriteString(w, "hello, "+mux.Vars(req)["file"]+"!\n")
// 	return
// }

type Application struct {
	Config  Configuration
	Mongo   *mgo.Session
	Manager *Manager
	Router  *mux.Router

	server   *Server
	shutdown chan os.Signal
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
	}
	app.Manager = NewManager(&app)
	app.Router = mux.NewRouter()
	app.Router.Handle("/streams", app.StreamsHandler()).Methods("POST")
	app.Router.Handle("/streams/info/{stream_id}", app.StreamInfoHandler()).Methods("GET")
	app.Router.Handle("/streams/activate", app.StreamActivateHandler()).Methods("POST")
	app.Router.Handle("/core/start", app.CoreStartHandler()).Methods("GET")
	app.Router.Handle("/core/frame", app.CoreFrameHandler()).Methods("POST")
	app.server = NewServer("127.0.0.1:12345", app.Router)

	return &app
}

type AppHandler func(http.ResponseWriter, *http.Request) (error, int)

func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err, code := fn(w, r); err != nil {
		http.Error(w, err.Error(), code)
	}
}

func (app *Application) DeactivateStreamService(s *Stream) error {
	return nil
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

// func (app *Application) AuthenticateCore(r *http.Request) (s *Stream, err error) {
// 	token := r.Header.Get("Authorization")
// 	s, err = app.Manager.Tokens.FindStream(token)
// 	return
// }

// Return a path indicating where stream files should be stored
func (app *Application) StreamDir(stream_id string) string {
	return filepath.Join(app.Config.Name+"_data", stream_id)
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
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
	app.Shutdown()
}

func (app *Application) Shutdown() {
	// Order matters.
	app.server.Close()
	app.Mongo.Close()
}

func (app *Application) StreamActivateHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
		if r.Header.Get("Authorization") != app.Config.Password {
			return errors.New("Unauthorized"), 401
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
			return errors.New("Bad request: " + err.Error()), 400
		}
		token, _, err := app.Manager.ActivateStream(msg.TargetId, msg.User, msg.Engine)
		if err != nil {
			return errors.New("Unable to activate stream: " + err.Error()), 400
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

func (app *Application) CoreFrameHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
		token := r.Header.Get("Authorization")
		md5String := r.Header.Get("Content-MD5")
		body, _ := ioutil.ReadAll(r.Body)
		h := md5.New()
		io.WriteString(h, string(body))
		if md5String != hex.EncodeToString(h.Sum(nil)) {
			return errors.New("MD5 mismatch"), 400
		}
		e := app.Manager.ModifyActiveStream(token, func(stream *Stream) error {
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
		if e != nil {
			return e, 400
		}
		return
	}
}

func (app *Application) CoreStartHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
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
				fmt.Println(err)
				return errors.New("Cannot load target's options")
			}
			rep.Options = mgoRes["options"]
			// Load the streams' files
			if stream.Frames > 0 {
				frameDir := filepath.Join(app.StreamDir(rep.StreamId), strconv.Itoa(stream.Frames))
				checkpointDirs, e := ioutil.ReadDir(frameDir)
				if e != nil {
					return errors.New("Cannot read frames directory")
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
			return e, 400
		}
		data, e := json.Marshal(rep)
		if e != nil {
			return e, 400
		}
		w.Write(data)
		return
	}
}

func (app *Application) StreamsHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
		user, err := app.CurrentUser(r)
		if err != nil {
			return errors.New("Unable to find user."), 401
		}
		isManager := app.IsManager(user)
		if isManager == false {
			return errors.New("Not a manager."), 401
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
			return errors.New("Bad request: " + err.Error()), 400
		}
		streamId := util.RandSeq(36)
		// Add files to disk
		stream := NewStream(streamId, msg.TargetId, "OK", 0, 0, int(time.Now().Unix()))
		todo := map[string]map[string]string{"files": msg.Files, "tags": msg.Tags}
		for Directory, Content := range todo {
			for filename, fileb64 := range Content {
				files_dir := filepath.Join(app.StreamDir(streamId), Directory)
				os.MkdirAll(files_dir, 0776)
				err = ioutil.WriteFile(filepath.Join(files_dir, filename), []byte(fileb64), 0776)
				if err != nil {
					return err, 400
				}
			}
		}
		cursor := app.Mongo.DB("streams").C(app.Config.Name)
		err = cursor.Insert(stream)
		if err != nil {
			// clean up
			os.RemoveAll(app.StreamDir(streamId))
			return errors.New("Unable insert stream into DB"), 400
		}
		// Insert stream into Manager after ensuring state is correct.
		e := app.Manager.AddStream(stream, msg.TargetId)
		if e != nil {
			return e, 400
		}
		data, _ := json.Marshal(map[string]string{"stream_id": streamId})
		w.Write(data)
		return
	}
}

func (app *Application) StreamInfoHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
		//cursor := app.Mongo.DB("streams").C(app.Config.Name)
		//msg := mongoStream{}
		streamId := mux.Vars(r)["stream_id"]
		var result []byte
		e := app.Manager.ReadStream(streamId, func(stream *Stream) error {
			result, err = json.Marshal(stream)
			return err
		})
		if e != nil {
			return e, 400
		}
		w.Write(result)
		return nil, 200
	}
}

// func (app *Application) CoreStartHandler() AppHandler {
// 	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
// 		as, err := app.AuthenticateCore(r)
// 		if err != nil {
// 			return errors.New("Bad Token"), 401
// 		}
// 		files, err := as.LoadCheckpointFiles(app.StreamDir(as.id))
// 		type Reply struct {
// 			StreamId string                 `json:"stream_id"`
// 			TargetId string                 `json:"target_id"`
// 			Files    map[string]string      `json:"files"`
// 			Options  map[string]interface{} `json:"options, string"`
// 		}
// 		rep := Reply{
// 			stream_id: as.id,
// 		}
// 		return
// 	}
// }
