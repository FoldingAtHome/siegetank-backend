package scv

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/gorilla/mux"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"../util"
)

func DownloadHandler(w http.ResponseWriter, req *http.Request) (err error) {
	fmt.Println("processing request...")
	time.Sleep(time.Duration(10) * time.Second)
	io.WriteString(w, "hello, "+mux.Vars(req)["file"]+"!\n")
	return
}

// If successful, returns a non-empty user_id
func AuthorizeManager(*http.Request) string {
	// check token to see if it's a manager's token or not
	return "diwaka"
}

type Application struct {
	Mongo        *mgo.Session
	ExternalHost string
	DataDir      string
	Password     string
	Name         string
	TM           *TargetManager
}

func NewApplication(name string) *Application {
	fmt.Print("Connecting to database... ")
	session, err := mgo.Dial("localhost:27017")
	if err != nil {
		panic(err)
	}
	fmt.Print("ok")
	app := Application{
		Mongo:        session,
		Password:     "12345",
		ExternalHost: "vspg11.stanford.edu",
		Name:         name,
		DataDir:      name + "_data",
		TM:           NewTargetManager(),
	}
	return &app
}

type AppHandler func(http.ResponseWriter, *http.Request) error

func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := fn(w, r); err != nil {
		http.Error(w, err.Error(), 500)
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

// Return a path indicating where stream files should be stored
func (app *Application) StreamDir(stream_id string) string {
	return filepath.Join(app.DataDir, stream_id)
}

func (app *Application) Run() {
	r := mux.NewRouter()
	r.Handle("/streams/{stream_id}", app.PostStreamHandler()).Methods("POST")

	http.Handle("/", r)
	err := http.ListenAndServe(":12345", nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

func (app *Application) Shutdown() {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, os.Kill)
	<-c
	app.Mongo.Close()
}

func (app *Application) PostStreamHandler() AppHandler {
	return func(w http.ResponseWriter, r *http.Request) (err error) {
		/*:
			-Validate Authorization token in the header.
			-Write the data to disk.
			-Validate that the requesting user is a manager.
			-Record stream statistics in MongoDB.

		        {
		            "target_id": "target_id",
		            "files": {"system.xml.gz.b64": "file1.b64",
		                "integrator.xml.gz.b64": "file2.b64",
		                "state.xml.gz.b64": "file3.b64"
		            }
		            "tags": {
		                "pdb.gz.b64": "file4.b64",
		            } // optional
		        }
		*/
		user, err := app.CurrentUser(r)
		if err != nil {
			return errors.New("Unable to find user.")
		}
		isManager := app.IsManager(user)
		if isManager == false {
			return errors.New("Unauthorized.")
		}
		type Message struct {
			TargetId string            `json:"target_id"`
			Files    map[string]string `json:"files"`
			Tags     map[string]string `json:"tags"`
		}
		msg := Message{}
		decoder := json.NewDecoder(r.Body)
		err = decoder.Decode(&msg)
		if err != nil {
			return errors.New("Bad request: " + err.Error())
		}
		stream_id := util.RandSeq(36)
		todo := map[string]map[string]string{"files": msg.Files, "tags": msg.Tags}
		for Directory, Content := range todo {
			for filename, fileb64 := range Content {
				filedata, err := base64.StdEncoding.DecodeString(fileb64)
				if err != nil {
					return errors.New("Unable to decode file")
				}
				files_dir := filepath.Join(app.StreamDir(stream_id), Directory)
				os.MkdirAll(files_dir, 0776)
				err = ioutil.WriteFile(filepath.Join(files_dir, filename), filedata, 0776)
				if err != nil {
					return err
				}
			}
		}
		type Stream struct {
			Id           string `bson:"_id"`
			Frames       int    `bson:"frames"`
			Status       string `bson:"status"`
			ErrorCount   int    `bson:"error_count"`
			CreationDate int    `bson:"creation_date"`
		}
		stream := Stream{
			Id:           stream_id,
			Status:       "OK",
			CreationDate: int(time.Now().Unix()),
		}
		cursor := app.Mongo.DB("streams").C(app.Name)
		err = cursor.Insert(stream)
		if err != nil {
			os.RemoveAll(app.StreamDir(stream_id))
			return errors.New("Unable insert stream into DB")
		}
		// Does nothing if the target already exists.
		app.TM.AddStreamToTarget(msg.TargetId, stream_id)
		return
	}
}
