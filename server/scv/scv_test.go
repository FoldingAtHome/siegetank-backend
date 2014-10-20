package scv

import (
	//"time"
	// "sync"
	"../util"
	"bytes"
	// "fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

var serverAddr string = "http://127.0.0.1/streams/wowsogood"

func (app *Application) _addUser(user string) (token string) {
	token = util.RandSeq(36)
	type Msg struct {
		Id    string `bson:"_id"`
		Token string `bson:"token"`
	}
	app.Mongo.DB("users").C("all").Insert(Msg{user, token})
	return
}

func (app *Application) _addManager(user string, weight int) (token string) {
	token = app._addUser(user)
	type Msg struct {
		Id     string `bson:"_id"`
		Weight int    `bson:"weight"`
	}
	app.Mongo.DB("users").C("managers").Insert(Msg{user, weight})
	return
}

func _init() *Application {
	app := NewApplication("testServer")
	db_names, _ := app.Mongo.DatabaseNames()
	for _, name := range db_names {
		app.Mongo.DB(name).DropDatabase()
	}
	return app
}

func (app *Application) _shutdown() {
	db_names, _ := app.Mongo.DatabaseNames()
	for _, name := range db_names {
		app.Mongo.DB(name).DropDatabase()
	}
	app.Shutdown()
}

func TestPostStreamUnauthorized(t *testing.T) {
	app := _init()
	defer app._shutdown()
	req, _ := http.NewRequest("POST", serverAddr, nil)
	w := httptest.NewRecorder()
	app.PostStreamHandler().ServeHTTP(w, req)
	assert.Equal(t, w.Code, 401)

	token := app._addUser("yutong")
	req, _ = http.NewRequest("POST", serverAddr, nil)
	req.Header.Add("Authorization", token)
	w = httptest.NewRecorder()
	app.PostStreamHandler().ServeHTTP(w, req)
	assert.Equal(t, w.Code, 401)
}

func TestPostBadStream(t *testing.T) {
	app := _init()
	defer app._shutdown()
	token := app._addManager("yutong", 1)

	jsonData := `{"target_id":"12345", "files": {"openmm": "ZmlsZWRhdG`
	dataBuffer := bytes.NewBuffer([]byte(jsonData))
	req, _ := http.NewRequest("POST", serverAddr, dataBuffer)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	app.PostStreamHandler().ServeHTTP(w, req)
	assert.Equal(t, w.Code, 400)
}

func TestPostStream(t *testing.T) {
	app := _init()
	defer app._shutdown()
	token := app._addManager("yutong", 1)

	jsonData := `{"target_id":"12345", "files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==", "amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	dataBuffer := bytes.NewBuffer([]byte(jsonData))
	req, _ := http.NewRequest("POST", serverAddr, dataBuffer)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	app.PostStreamHandler().ServeHTTP(w, req)
	assert.Equal(t, w.Code, 200)
}
