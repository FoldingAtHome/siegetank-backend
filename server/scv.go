package main

import(
    "io"
    "net/http"
    "log"
    "time"
    "fmt"
    // "encoding/json"
    // "./scv"
    "github.com/gorilla/mux"
)


func DownloadHandler(w http.ResponseWriter, req *http.Request) {
    fmt.Println("processing request...")
    time.Sleep(time.Duration(10)*time.Second)
    io.WriteString(w, "hello, "+mux.Vars(req)["file"]+"!\n")
}

// func main() {

//     r := mux.NewRouter()
//     r.HandleFunc("/download/{file:.*}", DownloadHandler).Methods("GET")
//     http.Handle("/", r)
//     err := http.ListenAndServe(":12345", nil)
//     if err != nil {
//         log.Fatal("ListenAndServe: ", err)
//     }

//     // tm := TargetManager{targets: make(map[string]Target),
//     //     tokenManager: TokenManager{
//     //         tokens: make(map[string]*ActiveStream),
//     //     },
//     // }
//     // tm.tokenManager.RemoveToken("asdf")
//     // fmt.Println("yay")
// }
