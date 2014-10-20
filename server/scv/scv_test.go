package scv

import (
	"time"
	// "sync"
	"../util"
	"bytes"
	"encoding/json"
	// "fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

var serverAddr string = "http://127.0.0.1/streams/wowsogood"

type Fixture struct {
	app *Application
}

func (f *Fixture) addUser(user string) (token string) {
	token = util.RandSeq(36)
	type Msg struct {
		Id    string `bson:"_id"`
		Token string `bson:"token"`
	}
	f.app.Mongo.DB("users").C("all").Insert(Msg{user, token})
	return
}

func (f *Fixture) addManager(user string, weight int) (token string) {
	token = f.addUser(user)
	type Msg struct {
		Id     string `bson:"_id"`
		Weight int    `bson:"weight"`
	}
	f.app.Mongo.DB("users").C("managers").Insert(Msg{user, weight})
	return
}

func NewFixture() *Fixture {
	config := Configuration{
		MongoURI:     "localhost:27017",
		Name:         "testServer",
		Password:     "hello",
		ExternalHost: "alexis.stanford.edu",
		InternalHost: "127.0.0.1",
	}
	f := Fixture{
		app: NewApplication(config),
	}
	db_names, _ := f.app.Mongo.DatabaseNames()
	for _, name := range db_names {
		f.app.Mongo.DB(name).DropDatabase()
	}
	return &f
}

func (f *Fixture) shutdown() {
	db_names, _ := f.app.Mongo.DatabaseNames()
	for _, name := range db_names {
		f.app.Mongo.DB(name).DropDatabase()
	}
	f.app.Shutdown()
}

func TestPostStreamUnauthorized(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	req, _ := http.NewRequest("POST", "/streams", nil)
	w := httptest.NewRecorder()
	f.app.PostStreamHandler().ServeHTTP(w, req)
	assert.Equal(t, w.Code, 401)

	token := f.addUser("yutong")
	req, _ = http.NewRequest("POST", "/streams", nil)
	req.Header.Add("Authorization", token)
	w = httptest.NewRecorder()

	f.app.Router.ServeHTTP(w, req)

	//app.PostStreamHandler().ServeHTTP(w, req)
	assert.Equal(t, w.Code, 401)
}

func TestPostBadStream(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)

	jsonData := `{"target_id":"12345", "files": {"openmm": "ZmlsZWRhdG`
	dataBuffer := bytes.NewBuffer([]byte(jsonData))
	req, _ := http.NewRequest("POST", "/streams", dataBuffer)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 400)
}

func (f *Fixture) getStream(stream_id string) (result mongoStream, code int) {
	req, _ := http.NewRequest("GET", "/streams/info/"+stream_id, nil)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	json.Unmarshal(w.Body.Bytes(), &result)
	code = w.Code
	return
}

func TestPostStream(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)

	start := int(time.Now().Unix())
	jsonData := `{"target_id":"12345",
				  "files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==", "amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	dataBuffer := bytes.NewBuffer([]byte(jsonData))
	req, _ := http.NewRequest("POST", "/streams", dataBuffer)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 200)
	stream_map := make(map[string]string)
	json.Unmarshal(w.Body.Bytes(), &stream_map)
	mgo_stream, code := f.getStream(stream_map["stream_id"])
	assert.Equal(t, code, 200)
	assert.Equal(t, "OK", mgo_stream.Status)
	assert.Equal(t, 0, mgo_stream.Frames)
	assert.Equal(t, 0, mgo_stream.ErrorCount)
	assert.Equal(t, false, mgo_stream.Active)
	assert.True(t, mgo_stream.CreationDate-start < 1)

	_, code = f.getStream("12345")
	assert.Equal(t, code, 400)

	// try adding tags
	jsonData = `{"target_id":"12345",
	     	     "files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==", "amber": "ZmlsZWRhdGFibGFoYmFsaA=="},
				 "tags": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	dataBuffer = bytes.NewBuffer([]byte(jsonData))
	req, _ = http.NewRequest("POST", "/streams", dataBuffer)
	req.Header.Add("Authorization", token)
	w = httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 200)
}
