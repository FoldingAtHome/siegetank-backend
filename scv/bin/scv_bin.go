package main

import (
	"../src"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

func main() {
	runtime.GOMAXPROCS(2 * runtime.NumCPU())
	conf := scv.Configuration{
		SSL: make(map[string]string),
	}
	var configFile = flag.String("config", "", "configuration file for the SCV")
	flag.Parse()
	fmt.Println(*configFile)
	config := ""
	if *configFile == "" {
		dir, _ := filepath.Abs(filepath.Dir(os.Args[0]))
		config = filepath.Join(dir, "scv.json")
	} else {
		config = *configFile
	}
	log.Println("Config file: ", config)
	file, err := os.Open(config)
	if err != nil {
		panic("Could not open config file.")
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&conf)
	app := scv.NewApplication(conf)
	app.Run()
}
