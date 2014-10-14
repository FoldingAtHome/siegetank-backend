package main

import(
    "io"
    "net/http"
    "log"
    "time"
    "fmt"
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
    return func(w http.ResponseWriter, r *http.Request) error {
        user := app.GetCurrentUser(r)
        if user == "" {
            return errors.New("Unauthorized")
        }
        fmt.Println(app.Mongo)
        fmt.Println(app.external_host)
        // use app.Mongo
        // handle authorization
        // handle Mongo transaction
        // write using application specific properties
        return nil
    }
}

type User struct {
    Id string `bson:"_id"`
    Token string
}

// Look up the user using the Authorization header
func (app *Application) GetCurrentUser(r *http.Request) string {
    token := r.Header.Get("Authorization")
    cursor := app.Mongo.DB("users").C("all")
    result := User{}
    err := cursor.Find(bson.M{"token": token}).One(&result)
    if err != nil {
        return ""
    }
    return result.Id
}

func (app *Application) IsManager(user_id string) string {
    cursor := app.Mongo.DB("")
}

cursor = self.motor.users.managers
        query = yield cursor.find_one({'_id': user})
        if query:
            return True
        else:
            return False

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
    //app.GetCurrentUser("some_token")
    app.Run()
}
