package main

import(
    "os"
    "io"
    "io/ioutil"
    "net/http"
    "log"
    "time"
    "fmt"
    "errors"
    "bytes"
    "path/filepath"
    "encoding/json"
    "encoding/base64"

    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "github.com/gorilla/mux"

    // "./scv"
    "./util"
)

func DownloadHandler(w http.ResponseWriter, req *http.Request) (err error) {
    fmt.Println("processing request...")
    time.Sleep(time.Duration(10)*time.Second)
    io.WriteString(w, "hello, "+mux.Vars(req)["file"]+"!\n")
    return
}

// If successful, returns a non-empty user_id
func AuthorizeManager(*http.Request) string {
    // check token to see if it's a manager's token or not
    return "diwaka"
}

type Application struct {
    Mongo *mgo.Session
    ExternalHost string
    DataDir string
    Password string
    Name string
}

func NewApplication(name string) *Application {
    fmt.Print("Connecting to database... ")
    session, err := mgo.Dial("localhost:27017")
    if err != nil {
            panic(err)
    }
    fmt.Print("ok")
    app := Application{
        Mongo: session,
        Password: "12345",
        ExternalHost: "vspg11.stanford.edu",
        Name: name,
        DataDir: name+"_data",
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

// Return a path indicati
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

func (app *Application) PostStreamHandler() AppHandler {
    return func(w http.ResponseWriter, r *http.Request) (err error) {
        /*:
        -Validate Authorization token in the header.
        -Validate that the requesting user is a manager.
        -Write the data to disk.
        -Record stream statistics in MongoDB.
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
            TargetId string `json:"target_id"`
            Files map[string]string `json:"files"`
            Tags map[string]string `json:"tags"`
        }
        msg := Message{}
        decoder := json.NewDecoder(r.Body)
        err = decoder.Decode(&msg)
        if err != nil {
            return errors.New("Bad request: "+err.Error())
        }
        target_id = msg.TargetId
        if target_id 
        stream_id := util.RandSeq(36)
        todo := map[string] map[string]string{"files": msg.Files, "tags": msg.Tags}
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
            Id string `bson:"_id"`
            Frames int `bson:"frames"`
            Status string `bson:"status"`
            ErrorCount int `bson:"error_count"`
            CreationDate int `bson:"creation_date"`
        }
        stream := Stream{
            Id: stream_id,
            Status: "OK",
            CreationDate: int(time.Now().Unix()),
        }
        cursor := app.Mongo.DB("streams").C(app.Name)
        err = cursor.Insert(stream)
        if err != nil {
            os.RemoveAll(app.StreamDir(stream_id))
            return errors.New("Unable to connect to database.")
        }
        return
    }
}

func main() {
    app := NewApplication("vspg11")
    req := func(token string, jsonData string) {
        time.Sleep(3*time.Second)
        client := &http.Client{}
        dataBuffer := bytes.NewBuffer([]byte(jsonData))
        req, _ := http.NewRequest("POST", "http://127.0.0.1:12345/streams/982034859", dataBuffer)
        req.Header.Add("Authorization", token)
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        } else {
            defer resp.Body.Close()
            data, _ := ioutil.ReadAll(resp.Body)
            fmt.Println(string(data))
        }
        //fmt.Println(resp.Body, err)
    }
    //go req("19762704-41c9-4752-9aaa-802098ffa02e")

    jsonData1 := `{"target_id":"12345", "files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==", "amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
    jsonData2 := `{"target_id":"12345",
                   "files": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==", "amber": "ZmlsZWRhdGFibGFoYmFsaA=="},
                   "tags": {"openmm": "ZmlsZWRhdGFibGFoYmFsaA==", "amber": "ZmlsZWRhdGFibGFoYmFsaA=="}}`
    // jsonData3 := `{"target_id":"12345", "files": "foo", "tags": {"oh": "wow"}}`
    go req("1d48d5df-780e-4083-95fa-c620a80cecb3", jsonData1)
    go req("1d48d5df-780e-4083-95fa-c620a80cecb3", jsonData2)
    // go req("1d48d5df-780e-4083-95fa-c620a80cecb3", jsonData3)
    app.Run()
}
