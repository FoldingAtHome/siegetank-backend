package scv

// import (
// 	"encoding/base64"
// 	"encoding/json"
// 	"errors"
// 	"fmt"
// 	// "io"
// 	"io/ioutil"
// 	"log"
// 	"net/http"
// 	"os"
// 	"os/signal"
// 	"path/filepath"
// 	"time"

// 	"github.com/gorilla/mux"
// 	"gopkg.in/mgo.v2"
// 	"gopkg.in/mgo.v2/bson"

// 	"../util"
// )

// var _ = fmt.Printf

// // func DownloadHandler(w http.ResponseWriter, req *http.Request) (err error) {
// // 	time.Sleep(time.Duration(10) * time.Second)
// // 	io.WriteString(w, "hello, "+mux.Vars(req)["file"]+"!\n")
// // 	return
// // }

// type Application struct {
// 	Config Configuration
// 	Mongo  *mgo.Session
// 	TM     *TargetManager
// 	Router *mux.Router

// 	server   *Server
// 	shutdown chan os.Signal
// }

// // SSL stuff later
// type Configuration struct {
// 	MongoURI     string
// 	Name         string
// 	Password     string
// 	ExternalHost string
// 	InternalHost string
// }

// func NewApplication(config Configuration) *Application {
// 	session, err := mgo.Dial(config.MongoURI)
// 	if err != nil {
// 		panic(err)
// 	}
// 	app := Application{
// 		Config: config,
// 		Mongo:  session,
// 		TM:     NewTargetManager(),
// 	}
// 	app.Router = mux.NewRouter()
// 	app.Router.Handle("/streams", app.StreamsHandler()).Methods("POST")
// 	app.Router.Handle("/streams/info/{stream_id}", app.GetStreamInfoHandler()).Methods("GET")
// 	app.Router.Handle("/streams/activate", app.StreamActivateHandler()).Methods("POST")
// 	app.server = NewServer("127.0.0.1:12345", app.Router)

// 	return &app
// }

// type AppHandler func(http.ResponseWriter, *http.Request) (error, int)

// func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
// 	if err, code := fn(w, r); err != nil {
// 		http.Error(w, err.Error(), code)
// 	}
// }

// // Look up the User using the Authorization header
// func (app *Application) CurrentUser(r *http.Request) (user string, err error) {
// 	token := r.Header.Get("Authorization")
// 	cursor := app.Mongo.DB("users").C("all")
// 	result := make(map[string]interface{})
// 	if err = cursor.Find(bson.M{"token": token}).One(&result); err != nil {
// 		return
// 	}
// 	user = result["_id"].(string)
// 	return
// }

// func (app *Application) IsManager(user string) bool {
// 	cursor := app.Mongo.DB("users").C("managers")
// 	result := make(map[string]interface{})
// 	if err := cursor.Find(bson.M{"_id": user}).One(&result); err != nil {
// 		return false
// 	} else {
// 		return true
// 	}
// }

// func (app *Application) AuthenticateCore(r *http.Request) (as *ActiveStream, err error) {
// 	token := r.Header.Get("Authorization")
// 	as, err = app.TM.Tokens.FindStream(token)
// 	return
// }

// // Return a path indicating where stream files should be stored
// func (app *Application) StreamDir(stream_id string) string {
// 	return filepath.Join(app.Config.Name+"_data", stream_id)
// }

// // Run starts the server. Listens and Serves asynchronously. And sets up necessary
// // signal handlers for graceful termination. This blocks until a signal is sent
// func (app *Application) Run() {
// 	go func() {
// 		err := app.server.ListenAndServe()
// 		if err != nil {
// 			log.Println("ListenAndServe: ", err)
// 		}
// 	}()
// 	c := make(chan os.Signal, 1)
// 	signal.Notify(c, os.Interrupt, os.Kill)
// 	<-c
// 	app.Shutdown()
// }

// func (app *Application) Shutdown() {
// 	// Order matters.
// 	app.server.Close()
// 	app.Mongo.Close()
// }

// type mongoStream struct {
// 	Id           string `bson:"_id"`
// 	Status       string `bson:"status",json:"status"`
// 	Frames       int    `bson:"frames",json:"frames"`
// 	ErrorCount   int    `bson:"error_count",json:"error_count"`
// 	Active       bool   `bson:"active",json:"active"`
// 	CreationDate int    `bson:"creation_date"`
// }

func (app *Application) StreamActivateHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {

		// parse

		fn := func(s *Stream) {

		}

		app.Manager.ActivateStream(msg.Token, msg.User, msg.Engine)

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

// func (app *Application) StreamsHandler() AppHandler {
// 	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
// 		user, err := app.CurrentUser(r)

// 		if err != nil {
// 			return errors.New("Unable to find user."), 401
// 		}
// 		isManager := app.IsManager(user)
// 		if isManager == false {
// 			return errors.New("Not a manager."), 401
// 		}
// 		type Message struct {
// 			TargetId string            `json:"target_id"`
// 			Files    map[string]string `json:"files"`
// 			Tags     map[string]string `json:"tags,omitempty"`
// 		}
// 		msg := Message{}
// 		decoder := json.NewDecoder(r.Body)
// 		err = decoder.Decode(&msg)
// 		if err != nil {
// 			return errors.New("Bad request: " + err.Error()), 400
// 		}
// 		stream_id := util.RandSeq(36)
// 		todo := map[string]map[string]string{"files": msg.Files, "tags": msg.Tags}
// 		for Directory, Content := range todo {
// 			for filename, fileb64 := range Content {
// 				filedata, err := base64.StdEncoding.DecodeString(fileb64)
// 				if err != nil {
// 					return errors.New("Unable to decode file"), 400
// 				}
// 				files_dir := filepath.Join(app.StreamDir(stream_id), Directory)
// 				os.MkdirAll(files_dir, 0776)
// 				err = ioutil.WriteFile(filepath.Join(files_dir, filename), filedata, 0776)
// 				if err != nil {
// 					return err, 400
// 				}
// 			}
// 		}
// 		stream := mongoStream{
// 			Id:           stream_id,
// 			Status:       "OK",
// 			Frames:       0,
// 			ErrorCount:   0,
// 			CreationDate: int(time.Now().Unix()),
// 		}
// 		cursor := app.Mongo.DB("streams").C(app.Config.Name)
// 		err = cursor.Insert(stream)
// 		if err != nil {
// 			os.RemoveAll(app.StreamDir(stream_id))
// 			return errors.New("Unable insert stream into DB"), 400
// 		}
// 		// Does nothing if the target already exists.
// 		app.TM.AddStreamToTarget(msg.TargetId, stream_id, 0)
// 		data, _ := json.Marshal(map[string]string{"stream_id": stream_id})
// 		w.Write(data)
// 		return
// 	}
// }

// func (app *Application) GetStreamInfoHandler() AppHandler {
// 	return func(w http.ResponseWriter, r *http.Request) (err error, code int) {
// 		cursor := app.Mongo.DB("streams").C(app.Config.Name)
// 		msg := mongoStream{}
// 		stream_id := mux.Vars(r)["stream_id"]
// 		merr := cursor.Find(bson.M{"_id": stream_id}).Select(bson.M{"_id": 0}).One(&msg)
// 		if merr != nil {
// 			return errors.New("Unable to load stream from DB"), 400
// 		}
// 		data, _ := json.Marshal(msg)
// 		w.Write(data)
// 		return
// 	}
// }

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
