package scv

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"../util"
	"github.com/stretchr/testify/assert"
	// "gopkg.in/mgo.v2/bson"
)

var _ = fmt.Printf

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

func (f *Fixture) addTarget(targetId, owner, options string) {
	type Msg struct {
		Id    string `bson:"_id"`
		Owner string `bson:"owner"`
	}
	f.app.Mongo.DB("data").C("targets").Insert(Msg{targetId, owner})
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
	os.RemoveAll(f.app.Config.Name + "_data")
	f.app.Shutdown()
}

func TestPostStreamUnauthorized(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	req, _ := http.NewRequest("POST", "/streams", nil)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 401)
	token := f.addUser("yutong")
	req, _ = http.NewRequest("POST", "/streams", nil)
	req.Header.Add("Authorization", token)
	w = httptest.NewRecorder()

	f.app.Router.ServeHTTP(w, req)

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

func (f *Fixture) download(token, streamId, file string) (data []byte) {
	base := "/streams/download/" + streamId + "/" + file
	req, _ := http.NewRequest("GET", base, nil)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	data = w.Body.Bytes()
	return
}

func (f *Fixture) activateStream(target_id, engine, user, cc_token string) (token string, code int) {
	type Message struct {
		TargetId string `json:"target_id"`
		Engine   string `json:"engine"`
		User     string `json:"user"`
	}
	msg := Message{target_id, engine, user}
	data, _ := json.Marshal(msg)
	req, _ := http.NewRequest("POST", "/streams/activate", bytes.NewBuffer(data))
	req.Header.Add("Authorization", cc_token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	code = w.Code
	if code != 200 {
		return
	}
	result := make(map[string]string)
	json.Unmarshal(w.Body.Bytes(), &result)
	token = result["token"]
	return
}

func (f *Fixture) getStream(stream_id string) (result Stream, code int) {
	req, _ := http.NewRequest("GET", "/streams/info/"+stream_id, nil)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	json.Unmarshal(w.Body.Bytes(), &result)
	code = w.Code
	return
}

func (f *Fixture) postFrame(token string, data string) (code int) {
	dataBuffer := bytes.NewBuffer([]byte(data))
	req, _ := http.NewRequest("POST", "/core/frame", dataBuffer)
	h := md5.New()
	io.WriteString(h, string(data))
	req.Header.Add("Authorization", token)
	req.Header.Add("Content-MD5", hex.EncodeToString(h.Sum(nil)))
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	code = w.Code
	if code != 200 {
		fmt.Println(w.Body)
	}
	return
}

func (f *Fixture) postCheckpoint(token string, data string) (code int) {
	dataBuffer := bytes.NewBuffer([]byte(data))
	req, _ := http.NewRequest("POST", "/core/checkpoint", dataBuffer)
	h := md5.New()
	io.WriteString(h, string(data))
	req.Header.Add("Authorization", token)
	req.Header.Add("Content-MD5", hex.EncodeToString(h.Sum(nil)))
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	code = w.Code
	if code != 200 {
		fmt.Println(w.Body)
	}
	return
}

func (f *Fixture) postStream(token string, data string) (stream_id string, code int) {
	dataBuffer := bytes.NewBuffer([]byte(data))
	req, _ := http.NewRequest("POST", "/streams", dataBuffer)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	code = w.Code
	if code != 200 {
		return
	}
	stream_map := make(map[string]string)
	json.Unmarshal(w.Body.Bytes(), &stream_map)
	stream_id = stream_map["stream_id"]
	return
}

// func (f *Fixture) stopStream(token string, data string) (code int) {
// 	req, _ := http.NewRequest("method", urlStr, body)
// }

func TestPostStream(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	start := int(time.Now().Unix())
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
		"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	stream_id, code := f.postStream(token, jsonData)
	assert.Equal(t, code, 200)
	mStream, code := f.getStream(stream_id)

	assert.Equal(t, code, 200)
	assert.Equal(t, "OK", mStream.Status)
	assert.Equal(t, 0, mStream.Frames)
	assert.Equal(t, 0, mStream.ErrorCount)
	assert.True(t, mStream.CreationDate-start < 1)

	_, code = f.getStream("12345")
	assert.Equal(t, code, 400)

	// try adding tags
	jsonData = `{"target_id":"12345",
	    "files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==", "amber": "ZmlsZWRhdGFibGFoYmFsaA=="},
		"tags": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	stream_id, code = f.postStream(token, jsonData)
	assert.Equal(t, code, 200)
}

func TestDownload(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "b123",
		"amber": "b234"}}`
	stream_id, _ := f.postStream(token, jsonData)

	// data, code := f.download("bad_token", stream_id, "files/openmm")
	// assert.Equal(t, code, 401)
	assert.Equal(t, f.download(token, stream_id, "files/openmm"), []byte("b123"))
	assert.Equal(t, f.download(token, stream_id, "files/amber"), []byte("b234"))

}

func TestPostStreamAsync(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	start := int(time.Now().Unix())
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			jsonData := `{"target_id":"12345",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
			stream_id, code := f.postStream(token, jsonData)
			assert.Equal(t, code, 200)
			mStream, code := f.getStream(stream_id)
			assert.Equal(t, code, 200)
			assert.Equal(t, "OK", mStream.Status)
			assert.Equal(t, 0, mStream.Frames)
			assert.Equal(t, 0, mStream.ErrorCount)
			assert.True(t, mStream.CreationDate-start < 1)
			wg.Done()
		}()
	}
	wg.Wait()
}

func TestFaultyStreamActivation(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	var mu sync.Mutex
	stream_ids := make([]string, 10, 10)
	var wg sync.WaitGroup
	target_id := "123456"
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
			stream_id, code := f.postStream(token, jsonData)
			mu.Lock()
			stream_ids = append(stream_ids, stream_id)
			mu.Unlock()
			assert.Equal(t, code, 200)
			wg.Done()
		}()
	}
	wg.Wait()
	_, code := f.activateStream(target_id, "a", "b", "bad_pass")
	assert.Equal(t, code, 401)
	_, code = f.activateStream("54321", "a", "b", f.app.Config.Password)
	assert.Equal(t, code, 400)
}

func TestStreamActivation(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	var mu sync.Mutex
	stream_ids := make(map[string]struct{})
	var wg sync.WaitGroup
	target_id := "123456"
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
			stream_id, code := f.postStream(token, jsonData)
			mu.Lock()
			stream_ids[stream_id] = struct{}{}
			mu.Unlock()
			assert.Equal(t, code, 200)
			wg.Done()
		}()
	}
	wg.Wait()

	tokens := make(map[string]struct{})

	// activate 10 times asynchronously
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			engine := util.RandSeq(12)
			user := util.RandSeq(12)
			token, code := f.activateStream(target_id, engine, user, f.app.Config.Password)
			assert.Equal(t, code, 200)
			tokens[token] = struct{}{}
			wg.Done()
		}()
	}
	wg.Wait()
	_, code := f.activateStream(target_id, "random", "guy", f.app.Config.Password)
	assert.Equal(t, code, 400)
}

func TestBadCoreStart(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	req, _ := http.NewRequest("GET", "/core/start", nil)
	req.Header.Add("Authorization", "bad_token")
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 400)
}

func TestStreamCycle(t *testing.T) {
	// Test POSTing frames, checkpoints, starting and stopping.
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	stream_id, code := f.postStream(auth_token, jsonData)
	token, code := f.activateStream(target_id, "a", "b", f.app.Config.Password)
	assert.Equal(t, code, 200)

	// test posting plaintext
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "12345"}}`), 200)
	assert.Equal(t, f.app.Manager.streams[stream_id].activeStream.bufferFrames, 1)
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "67890"}}`), 200)
	assert.Equal(t, f.app.Manager.streams[stream_id].activeStream.bufferFrames, 2)
	assert.Equal(t, f.download(auth_token, stream_id, "buffer_files/some_file"), []byte("1234567890"))

	assert.Equal(t, f.postCheckpoint(token, `{"files": {"chkpt": "data"}, "frames": 0.234}`), 200)
	assert.Equal(t, f.app.Manager.streams[stream_id].activeStream.donorFrames, 0.234)
	assert.Equal(t, f.app.Manager.streams[stream_id].activeStream.bufferFrames, 0)

	assert.Equal(t, f.download(auth_token, stream_id, "2/0/some_file"), []byte("1234567890"))
	assert.Equal(t, f.download(auth_token, stream_id, "2/0/checkpoint_files/chkpt"), []byte("data"))

	assert.Equal(t, f.postCheckpoint(token, `{"files": {"chkpt": "data"}, "frames": 0.123}`), 200)
	assert.Equal(t, f.app.Manager.streams[stream_id].activeStream.donorFrames, 0.234+0.123)
	assert.Equal(t, f.app.Manager.streams[stream_id].activeStream.bufferFrames, 0)
	assert.Equal(t, f.download(auth_token, stream_id, "2/1/checkpoint_files/chkpt"), []byte("data"))

	// test posting base64 encoded
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file.b64": "MTIzNDU="}}`), 200)
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file.b64": "Njc4OTA="}}`), 200)
	assert.Equal(t, f.download(auth_token, stream_id, "buffer_files/some_file"), []byte("1234567890"))
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file.gz.b64": "H4sIAOX+dVQC/zM0MjYxBQAcOvXLBQAAAA=="}}`), 200)
	assert.Equal(t, f.download(auth_token, stream_id, "buffer_files/some_file"), []byte("123456789012345"))

}

func TestCoreStart(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	f.postStream(auth_token, jsonData)
	token, code := f.activateStream(target_id, "a", "b", f.app.Config.Password)
	assert.Equal(t, code, 200)

	{
		req, _ := http.NewRequest("GET", "/core/start", nil)
		req.Header.Add("Authorization", token)
		w := httptest.NewRecorder()
		f.app.Router.ServeHTTP(w, req)
		fmt.Println("??", w.Body)
		assert.Equal(t, w.Code, 200)
	}

	{
		dataBuffer := bytes.NewBuffer([]byte("12345678"))
		req, _ := http.NewRequest("POST", "/core/frame", dataBuffer)
		req.Header.Add("Authorization", token)
		req.Header.Add("Content-MD5", "1234")
		w := httptest.NewRecorder()
		f.app.Router.ServeHTTP(w, req)
		assert.Equal(t, w.Code, 400)
	}

	assert.Equal(t, f.postFrame(token, "12345678"), 400)
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "some_data"}}`), 200)
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "some_data"}}`), 400)
}
