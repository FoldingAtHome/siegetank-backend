package main

import (
	// "encoding/base64"
	// "encoding/json"
	// "errors"
	"fmt"
	// "io"
	// "io/ioutil"
	// "log"
	// "net/http"
	"os"
	"path/filepath"
	// "time"

	// "bytes"
	// "math/rand"
	"runtime"

	"encoding/json"

	"../../scv"
)

func main() {

	runtime.GOMAXPROCS(5)
	conf := scv.Configuration{
		SSL: make(map[string]string),
	}

	dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
	fmt.Println(dir)

	file, err := os.Open(filepath.Join(dir, "scv.json"))
	if err != nil {
		panic("Could not open config file.")
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&conf)

	fmt.Println(conf)

	app := scv.NewApplication(conf)

	app.Run()
}
