package scv

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"../util"
	"github.com/stretchr/testify/assert"
	"gopkg.in/mgo.v2/bson"
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
	os.RemoveAll(f.app.Config.Name + "_data")
	go f.app.RecordDeferredDocs()
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
	assert.Equal(t, w.Code, 400)
	token := f.addUser("yutong")
	req, _ = http.NewRequest("POST", "/streams", nil)
	req.Header.Add("Authorization", token)
	w = httptest.NewRecorder()

	f.app.Router.ServeHTTP(w, req)

	assert.Equal(t, w.Code, 400)
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
	if w.Code == 200 {
		data = w.Body.Bytes()
	} else {
		data = make([]byte, 0)
	}
	return
}

func (f *Fixture) downloadFrame(token, streamId, file string, frame int) (data []byte) {
	return f.download(token, streamId, strconv.Itoa(frame)+"/0/"+file)
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

type testStream struct {
	Stream
	Active bool `json:"active"`
}

func (f *Fixture) getStream(stream_id string) (result testStream, code int) {
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

type SyncResult struct {
	Partitions      []int    `json:"partitions"`
	SeedFiles       []string `json:"seed_files"`
	FrameFiles      []string `json:"frame_files"`
	CheckpointFiles []string `json:"checkpoint_files"`
}

func (f *Fixture) syncStream(token, streamId string) (SyncResult, int) {
	req, _ := http.NewRequest("GET", "/streams/sync/"+streamId, nil)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	result := SyncResult{
		Partitions:      make([]int, 0),
		SeedFiles:       make([]string, 0),
		FrameFiles:      make([]string, 0),
		CheckpointFiles: make([]string, 0),
	}
	er := json.Unmarshal(w.Body.Bytes(), &result)
	if er != nil {
		panic("syncStream failure")
	}
	return result, w.Code
}

func (f *Fixture) streamStop(token, streamId string) int {
	req, _ := http.NewRequest("PUT", "/streams/stop/"+streamId, nil)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	return w.Code
}

func (f *Fixture) streamStart(token, streamId string) int {
	req, _ := http.NewRequest("PUT", "/streams/start/"+streamId, nil)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	return w.Code
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

func (f *Fixture) deleteStream(token, streamId string) (code int) {
	req, _ := http.NewRequest("PUT", "/streams/delete/"+streamId, nil)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	return w.Code
}

func (f *Fixture) coreHeartbeat(token string) (code int) {
	req, _ := http.NewRequest("POST", "/core/heartbeat", nil)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	return w.Code
}

func (f *Fixture) coreStop(token string, error_string string) (code int) {
	body := `{"error": "` + error_string + `"}`
	req, _ := http.NewRequest("PUT", "/core/stop", bytes.NewBuffer([]byte(body)))
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	return w.Code
}

func (f *Fixture) coreStart(token string) (streamId string, code int) {
	req, _ := http.NewRequest("GET", "/core/start", nil)
	req.Header.Add("Authorization", token)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	code = w.Code
	if code != 200 {
		return
	}
	stream_map := make(map[string]interface{})
	json.Unmarshal(w.Body.Bytes(), &stream_map)
	streamId = stream_map["stream_id"].(string)
	return
}

func (f *Fixture) loadMongoStream(stream_id string) map[string]interface{} {
	cursor := f.app.Mongo.DB("streams").C(f.app.Config.Name)
	result := make(map[string]interface{})
	cursor.Find(bson.M{"_id": stream_id}).One(&result)
	return result
}

func TestRegisterSCV(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	f.app.RegisterSCV()
	config := Configuration{}
	cursor := f.app.Mongo.DB("servers").C("scvs")
	cursor.FindId(f.app.Config.Name).One(&config)
	assert.Equal(t, config.Name, f.app.Config.Name)
	assert.Equal(t, config.ExternalHost, f.app.Config.ExternalHost)
	assert.Equal(t, config.Password, f.app.Config.Password)
}

func TestLoadStreamsSuccess(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
		"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	f.postStream(token, jsonData)
	f.app.Manager = NewManager(f.app)
	defer func() {
		if recover() != nil {
			assert.True(t, false)
		}
	}()
	f.app.LoadStreams()
}

func TestLoadStreamsExistsInMongo(t *testing.T) {
	// Streams that exist in Mongo but not on disk throws a panic.
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
		"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	stream_id, _ := f.postStream(token, jsonData)

	os.RemoveAll(f.app.StreamDir(stream_id))

	defer func() {
		if recover() != nil {
			// passed
		} else {
			// failed
			assert.True(t, false)
		}
	}()
	f.app.LoadStreams()
}

func TestLoadStreamsExistsOnDisk(t *testing.T) {
	// Streams that exist on disk but not in Mongo are deleted
	f := NewFixture()
	defer f.shutdown()

	someString := "blah"

	os.MkdirAll(f.app.StreamDir("1234"), 0776)
	err := ioutil.WriteFile(filepath.Join(f.app.StreamDir("1234"), "output.txt"), []byte(someString), 0776)
	assert.Nil(t, err)
	_, err = ioutil.ReadDir(f.app.StreamDir("1234"))
	assert.Nil(t, err)
	f.app.LoadStreams()
	_, err = ioutil.ReadDir(f.app.StreamDir("1234"))
	assert.NotNil(t, err)
}

func TestLoadStreamsInconsistentFrames(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	stream_id, code := f.postStream(auth_token, jsonData)
	token, code := f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
	assert.Equal(t, code, 200)
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "12345"}}`), 200)
	assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "67890"}}`), 200)
	assert.Equal(t, f.postCheckpoint(token, `{"files": {"chkpt": "data"}, "frames": 0.234}`), 200)

	// manually hack Mongo to use a different frame count.
	err := f.app.StreamsCursor().UpdateId(stream_id, bson.M{"$set": bson.M{"frames": "50"}})
	assert.Nil(t, err)
	stream, code := f.getStream(stream_id)
	assert.Equal(t, code, 200)
	f.app.Manager = NewManager(f.app)
	f.app.LoadStreams()
	stream, code = f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, stream.Frames, 2)
}

func TestLoadDisabledStreams(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	stream_id, _ := f.postStream(auth_token, jsonData)
	assert.Equal(t, f.streamStop(auth_token, stream_id), 200)
	f.app.Manager = NewManager(f.app)
	f.app.LoadStreams()
	stream, code := f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, stream.MongoStatus, "disabled")
	targetImpl := f.app.Manager.targets[target_id]
	assert.Equal(t, len(targetImpl.disabledStreams), 1)
	assert.Equal(t, targetImpl.inactiveStreams.Len(), 0)
}

func TestLoadStreamsErrorCount(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	stream_id, code := f.postStream(auth_token, jsonData)
	for i := 0; i < MAX_STREAM_FAILS; i++ {
		token, code := f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
		assert.Equal(t, code, 200)
		assert.Equal(t, f.coreStop(token, "some_error"), 200)
	}
	// It takes some time to insert the stream's status into Mongo
	time.Sleep(1 * time.Second)
	f.app.Manager = NewManager(f.app)
	f.app.LoadStreams()
	stream, code := f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, stream.MongoStatus, "disabled")
	assert.Equal(t, stream.ErrorCount, MAX_STREAM_FAILS)
}

func TestLoadStreamsErrorCountOK(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	stream_id, code := f.postStream(auth_token, jsonData)
	for i := 0; i < 3; i++ {
		token, code := f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
		assert.Equal(t, code, 200)
		assert.Equal(t, f.coreStop(token, "some_error"), 200)
	}
	// It takes some time to insert the stream's status into Mongo
	time.Sleep(1 * time.Second)
	f.app.Manager = NewManager(f.app)
	f.app.LoadStreams()
	stream, code := f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, stream.MongoStatus, "enabled")
	assert.Equal(t, stream.ErrorCount, 3)
}

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
	// assert.Equal(t, "OK", mStream.Status)
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

	cursor := f.app.Mongo.DB("streams").C(f.app.Config.Name)
	result := make(map[string]interface{})
	cursor.Find(bson.M{"_id": stream_id}).One(&result)

	assert.Equal(t, result["frames"].(int), 0)
	assert.Equal(t, result["error_count"].(int), 0)
	assert.Equal(t, result["status"].(string), "enabled")
	assert.Equal(t, result["target_id"].(string), "12345")
	assert.True(t, int(time.Now().Unix())-result["creation_date"].(int) < 1)
}

func TestDeleteStreamMulti(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "b123",
		"amber": "b234"}}`
	stream1, _ := f.postStream(token, jsonData)
	stream2, _ := f.postStream(token, jsonData)
	assert.Equal(t, f.deleteStream(token, stream1), 200)
	time.Sleep(time.Second)
	count, _ := f.app.StreamsCursor().Count()
	assert.Equal(t, count, 1)
	assert.Equal(t, f.deleteStream(token, stream2), 200)
	time.Sleep(time.Second)
	count, _ = f.app.StreamsCursor().Count()
	assert.Equal(t, count, 0)

}
func TestDeleteStream(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "b123",
		"amber": "b234"}}`
	stream_id, _ := f.postStream(token, jsonData)
	assert.Equal(t, f.deleteStream(token, stream_id), 200)
	assert.Equal(t, len(f.app.Manager.streams), 0)
	assert.Equal(t, len(f.app.Manager.targets), 0)
	time.Sleep(time.Second)
	count, _ := f.app.StreamsCursor().Count()
	assert.Equal(t, count, 0)
}

func TestDeleteActiveStream(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	token := f.addManager("yutong", 1)
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "b123",
		"amber": "b234"}}`
	stream_id, _ := f.postStream(token, jsonData)
	f.activateStream("12345", "a", "b", f.app.Config.Password)
	assert.Equal(t, f.deleteStream(token, stream_id), 200)
	assert.Equal(t, len(f.app.Manager.streams), 0)
	assert.Equal(t, len(f.app.Manager.targets), 0)
	time.Sleep(time.Second)
	count, _ := f.app.StreamsCursor().Count()
	assert.Equal(t, count, 0)
}

func TestDeleteDisabledStream(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	auth_token := f.addManager("yutong", 1)
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"12345",
		"files": {"openmm": "b123",
		"amber": "b234"}}`
	stream_id, _ := f.postStream(auth_token, jsonData)
	for i := 0; i < MAX_STREAM_FAILS; i++ {
		token, code := f.activateStream("12345", "some_engine", "some_donor", f.app.Config.Password)
		assert.Equal(t, code, 200)
		assert.Equal(t, f.coreStop(token, "some_error"), 200)
	}
	_, code := f.activateStream("12345", "some_engine", "some_donor", f.app.Config.Password)
	assert.Equal(t, code, 400)
	assert.Equal(t, f.deleteStream(auth_token, stream_id), 200)
	assert.Equal(t, len(f.app.Manager.streams), 0)
	assert.Equal(t, len(f.app.Manager.targets), 0)
	time.Sleep(time.Second)
	count, _ := f.app.StreamsCursor().Count()
	assert.Equal(t, count, 0)
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
	// assert.Equal(t, code, 400)
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
			// assert.Equal(t, "OK", mStream.Status)
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
	assert.Equal(t, code, 400)
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
			mu.Lock()
			tokens[token] = struct{}{}
			mu.Unlock()
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

func TestHammerTime(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()

	auth_token := f.addManager("yutong", 1)

	streams := make([]string, 0)
	nTargets := 5
	nStreams := 20
	nActivations := 29
	nCycles := 12

	for j := 0; j < nTargets; j++ {
		target_id := "target_" + strconv.Itoa(j)
		jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
		f.addTarget(target_id, "yutong", `{"options": {"steps_per_frame": 1}}`)
		for i := 0; i < nStreams; i++ {
			stream_id, code := f.postStream(auth_token, jsonData)
			assert.Equal(t, code, 200)
			streams = append(streams, stream_id)
		}
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	frameCounts := make(map[string]int)
	consecutiveCheckpoints := make(map[string]int)
	for j := 0; j < nTargets; j++ {
		target_id := "target_" + strconv.Itoa(j)
		for i := 0; i < nStreams; i++ {
			wg.Add(1)
			go func() {
				for j := 0; j < nActivations; j++ {
					token, code := f.activateStream(target_id, "some_engine", "some_user", f.app.Config.Password)
					assert.Equal(t, code, 200)
					streamId, code := f.coreStart(token)
					for i := 0; i < nCycles; i++ {
						var concatBin string
						fCount := rand.Intn(4)
						for j := 0; j < fCount; j++ {
							data := util.RandSeq(10)
							concatBin += data
							assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "`+data+`"}}`), 200)
							time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
							assert.Equal(t, f.coreHeartbeat(token), 200)
							time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
						}
						mu.Lock()
						frameCounts[streamId] += fCount
						nFrames := frameCounts[streamId]
						if fCount == 0 {
							consecutiveCheckpoints[streamId] += 1
						} else {
							consecutiveCheckpoints[streamId] = 0
						}
						consChkpt := consecutiveCheckpoints[streamId]
						mu.Unlock()
						assert.Equal(t, f.postCheckpoint(token, `{"files": {"chkpt": "data"}, "frames": 0.234}`), 200)

						if fCount > 0 {
							frameBin := f.downloadFrame(auth_token, streamId, "some_file", nFrames)
							if concatBin != string(frameBin) {
								fmt.Println(streamId, nFrames, "EXPECTED", concatBin, "GOT", string(frameBin))
							}
							assert.Equal(t, concatBin, string(frameBin))
						}
						url := strconv.Itoa(nFrames) + "/" + strconv.Itoa(consChkpt) + "/checkpoint_files/chkpt"
						chkptBin := f.download(auth_token, streamId, url)
						assert.Equal(t, "data", string(chkptBin))
					}
					assert.Equal(t, f.coreStop(token, ""), 200)
				}
				wg.Done()
			}()
		}
	}
	wg.Wait()
}

func TestStreamCheckpoint(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	streamId, code := f.postStream(auth_token, jsonData)
	token, code := f.activateStream(target_id, "a", "b", f.app.Config.Password)
	assert.Equal(t, code, 200)
	assert.Equal(t, f.postCheckpoint(token, `{"files": {"chkpt": "data1"}, "frames": 0.234}`), 200)
	assert.Equal(t, f.postCheckpoint(token, `{"files": {"chkpt": "data2"}, "frames": 0.234}`), 200)
	chkptBin := f.download(auth_token, streamId, "0/1/checkpoint_files/chkpt")
	assert.Equal(t, string(chkptBin), "data1")
	chkptBin = f.download(auth_token, streamId, "0/2/checkpoint_files/chkpt")
	assert.Equal(t, string(chkptBin), "data2")
}

func TestStreamStateActive(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	stream_id, code := f.postStream(auth_token, jsonData)

	stream, code := f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, stream.MongoStatus, "enabled")
	assert.Equal(t, stream.Active, false)
	token, code := f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
	assert.Equal(t, code, 200)
	stream, code = f.getStream(stream_id)
	assert.Equal(t, stream.MongoStatus, "enabled")
	assert.Equal(t, stream.Active, true)
	// stopping a core without an error message
	assert.Equal(t, f.coreStop(token, ""), 200)
	for i := 0; i < MAX_STREAM_FAILS; i++ {
		token, code = f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
		assert.Equal(t, code, 200)
		assert.Equal(t, f.coreStop(token, "some_error"), 200)
	}

	stream, code = f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, stream.MongoStatus, "disabled")
	assert.Equal(t, stream.ErrorCount, MAX_STREAM_FAILS)

	_, code = f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
	assert.Equal(t, code, 400)
	time.Sleep(time.Second * 2)
	result := f.loadMongoStream(stream_id)
	assert.Equal(t, result["frames"].(int), 0)
	assert.Equal(t, result["error_count"].(int), MAX_STREAM_FAILS)
	assert.Equal(t, result["status"].(string), "disabled")
}

func TestStreamSync(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	seed_files := map[string]string{
		"state.xml.gz.b64":  "ZmlsZWRhdGFibGFoYmFsaA==",
		"system.xml.gz.b64": "23lkjlakjoaweiuroaiweu==",
	}
	jsonData := map[string]interface{}{
		"target_id": target_id,
		"files":     seed_files,
	}
	auth_token := f.addManager("yutong", 1)
	data, _ := json.Marshal(jsonData)
	stream_id, code := f.postStream(auth_token, string(data))

	result, code := f.syncStream(auth_token, stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, result.Partitions, []int{})
	assert.Equal(t, result.SeedFiles, []string{"state.xml.gz.b64", "system.xml.gz.b64"})
	assert.Equal(t, result.CheckpointFiles, []string{})
	assert.Equal(t, result.FrameFiles, []string{})

	// Post some frames
	token, code := f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
	frame_files := map[string]string{
		"f1": "1234",
		"f2": "2345",
	}
	frameData, _ := json.Marshal(map[string]interface{}{"files": frame_files})
	assert.Equal(t, f.postFrame(token, string(frameData)), 200)
	assert.Equal(t, f.coreStop(token, ""), 200)

	result, code = f.syncStream(auth_token, stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, result.Partitions, []int{})
	assert.Equal(t, result.SeedFiles, []string{"state.xml.gz.b64", "system.xml.gz.b64"})
	assert.Equal(t, result.CheckpointFiles, []string{})
	assert.Equal(t, result.FrameFiles, []string{})

	token, code = f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)

	assert.Equal(t, f.postFrame(token, string(frameData)), 200)
	chkpt_files := map[string]string{
		"c1": "1234",
		"c2": "2345",
	}
	checkpointData, _ := json.Marshal(map[string]interface{}{"files": chkpt_files})
	assert.Equal(t, f.postCheckpoint(token, string(checkpointData)), 200)

	result, code = f.syncStream(auth_token, stream_id)
	assert.Equal(t, result.Partitions, []int{1})
	assert.Equal(t, result.SeedFiles, []string{"state.xml.gz.b64", "system.xml.gz.b64"})
	assert.Equal(t, result.CheckpointFiles, []string{"c1", "c2"})
	assert.Equal(t, result.FrameFiles, []string{"f1", "f2"})

	for i := 0; i < 10; i++ {
		frame_files := map[string]string{
			"f1": "1234",
			"f2": util.RandSeq(12),
		}
		frameData, _ := json.Marshal(map[string]interface{}{"files": frame_files})
		assert.Equal(t, f.postFrame(token, string(frameData)), 200)
	}
	assert.Equal(t, f.postCheckpoint(token, string(checkpointData)), 200)

	result, code = f.syncStream(auth_token, stream_id)
	assert.Equal(t, result.Partitions, []int{1, 11})
	assert.Equal(t, result.SeedFiles, []string{"state.xml.gz.b64", "system.xml.gz.b64"})
	assert.Equal(t, result.CheckpointFiles, []string{"c1", "c2"})
	assert.Equal(t, result.FrameFiles, []string{"f1", "f2"})

	assert.Equal(t, f.coreStop(token, ""), 200)

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
	start_time := int(time.Now().Unix())
	token, code := f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
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

	assert.Equal(t, f.coreStop(token, ""), 200)

	end_time := int(time.Now().Unix())

	time.Sleep(time.Second * 1)

	// check mongo stats
	cursor := f.app.Mongo.DB("stats").C(target_id)
	result := make(map[string]interface{})
	cursor.Find(bson.M{"stream": stream_id}).One(&result)
	assert.Equal(t, result["frames"].(float64), 0.234+0.123)
	assert.Equal(t, result["engine"].(string), "some_engine")
	assert.Equal(t, result["user"].(string), "some_donor")
	assert.True(t, math.Abs(float64(result["start_time"].(int)-start_time)) < 1)
	assert.True(t, math.Abs(float64(result["end_time"].(int)-end_time)) < 1)

	// check mongo stream
	cursor = f.app.Mongo.DB("streams").C(f.app.Config.Name)
	result = make(map[string]interface{})
	cursor.Find(bson.M{"_id": stream_id}).One(&result)
	assert.Equal(t, result["frames"].(int), 2)
	assert.Equal(t, result["error_count"].(int), 0)
	// assert.Equal(t, result["frames"].(int), 5)
	// assert.Equal(t, result["engine"].(string), "some_engine")
	// assert.Equal(t, result["user"].(string), "some_donor")

	assert.Equal(t, f.postFrame(token, `{"files": {"some_file": "12345"}}`), 400)
	assert.Equal(t, f.postCheckpoint(token, `{"files": {"chkpt": "data"}, "frames": 0.234}`), 400)
	assert.Nil(t, f.app.Manager.streams[stream_id].activeStream, nil)

	assert.Equal(t, f.download(auth_token, stream_id, "buffer_files/some_file"), []byte("123456789012345"))
	// test that activating a stream removes buffer_files
	token, code = f.activateStream(target_id, "a", "b", f.app.Config.Password)
	assert.Equal(t, f.download(auth_token, stream_id, "buffer_files/some_file"), []byte(""))
}

func TestStreamStartStop(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	target_id := "12345"
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	stream_id, code := f.postStream(auth_token, jsonData)

	result, code := f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, result.MongoStatus, "enabled")

	_, code = f.activateStream(target_id, "some_engine", "some_donor", f.app.Config.Password)
	assert.Equal(t, code, 200)

	assert.Equal(t, f.streamStop(auth_token, stream_id), 200)
	result, code = f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, result.MongoStatus, "disabled")

	assert.Equal(t, f.streamStart(auth_token, stream_id), 200)
	result, code = f.getStream(stream_id)
	assert.Equal(t, code, 200)
	assert.Equal(t, result.MongoStatus, "enabled")
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

func TestCoreExpiration(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	f.app.Manager.expirationTime = 5
	target_id := "12345"
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	f.postStream(auth_token, jsonData)
	token, code := f.activateStream(target_id, "a", "b", f.app.Config.Password)
	assert.Equal(t, code, 200)
	time.Sleep(time.Duration(6) * time.Second)
	assert.Equal(t, f.coreStop(token, ""), 400)
}

func TestCoreHeartbeat(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	f.app.Manager.expirationTime = 5
	target_id := "12345"
	f.addTarget("12345", "yutong", `{"options": {"steps_per_frame": 1}}`)
	jsonData := `{"target_id":"` + target_id + `",
				"files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==",
				"amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
	auth_token := f.addManager("yutong", 1)
	f.postStream(auth_token, jsonData)
	token, code := f.activateStream(target_id, "a", "b", f.app.Config.Password)
	time.Sleep(time.Duration(3) * time.Second)
	assert.Equal(t, code, 200)
	assert.Equal(t, f.coreHeartbeat(token), 200)
	time.Sleep(time.Duration(3) * time.Second)
	assert.Equal(t, f.coreStop(token, ""), 200)
	time.Sleep(time.Duration(3) * time.Second)
	assert.Equal(t, f.coreStop(token, ""), 400)
}

func TestAlive(t *testing.T) {
	f := NewFixture()
	defer f.shutdown()
	req, _ := http.NewRequest("GET", "/", nil)
	w := httptest.NewRecorder()
	f.app.Router.ServeHTTP(w, req)
	assert.Equal(t, w.Code, 200)
}
