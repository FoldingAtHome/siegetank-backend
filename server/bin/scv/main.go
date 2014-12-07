package main

import (
	"../../scv"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
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
