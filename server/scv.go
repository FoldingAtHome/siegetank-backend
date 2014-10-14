package main

import(
    "io"
    "io/ioutil"
    "net/http"
    "log"
    "time"
    "fmt"
    "errors"

    "gopkg.in/mgo.v2"
    "gopkg.in/mgo.v2/bson"
    "github.com/gorilla/mux"
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
    external_host string
    data_dir string
    password string
    name string
}

func NewApplication() *Application {
    fmt.Print("Connecting to database... ")
    session, err := mgo.Dial("localhost:27017")
    if err != nil {
            panic(err)
    }
    fmt.Print("ok")
    app := Application{
        Mongo: session,
        password: "12345",
        external_host: "vspg11.stanford.edu",
        name: "vspg11",
    }
    return &app
}

type AppHandler func(http.ResponseWriter, *http.Request) error

func (fn AppHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
    if err := fn(w, r); err != nil {
        http.Error(w, err.Error(), 500)
    }
}

func (app *Application) PostStreamHandler() AppHandler {
    return func(w http.ResponseWriter, r *http.Request) (err error) {
        /* Multi-Stage Process:

        -Validate that the user is indeed a user
        -Validate that the user is a manager
        -

        */
        user, err := app.CurrentUser(r)
        if err != nil {
            return errors.New("Unable to find user.")
        }
        fmt.Println("Authorized as user:", user)
        isManager := app.IsManager(user)
        fmt.Println("Is the user a manager?", isManager)
        // fmt.Println(app.Mongo)
        // fmt.Println(app.external_host)
        // use app.Mongo
        // handle authorization
        // handle Mongo transaction
        // write using application specific properties

        w.Write([]byte("logged in as: "+ user))
        return
    }
}

// Look up the User using the Authorization header
func (app *Application) CurrentUser(r *http.Request) (user string, err error) {
    token := r.Header.Get("Authorization")
    cursor := app.Mongo.DB("users").C("all")
    result := make(map[string]string)
    if err = cursor.Find(bson.M{"token": token}).One(&result); err != nil {
        return 
    }
    user = result["_id"]
    return
}

func (app *Application) IsManager(user string) bool {
    cursor := app.Mongo.DB("users").C("managers")
    result := make(map[string]string)
    if err := cursor.Find(bson.M{"_id": user}).One(&result); err != nil {
        return false
    } else {
        return true
    }
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

func main() {
    app := NewApplication()
    req := func(token string) {
        time.Sleep(3*time.Second)
        client := &http.Client{}
        req, _ := http.NewRequest("POST", "http://127.0.0.1:12345/streams/982034859", nil)
        req.Header.Add("Authorization", token)
        resp, err := client.Do(req)
        if err != nil {
            panic(err)
        } else {
            defer resp.Body.Close()
            contents, _ := ioutil.ReadAll(resp.Body)
            fmt.Println(string(contents))
        }
        fmt.Println(resp.Body, err)
    }

    app.Run()
}
