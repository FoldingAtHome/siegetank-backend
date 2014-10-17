package main

import (
	// "encoding/base64"
	// "encoding/json"
	// "errors"
	"fmt"
	// "io"
	"io/ioutil"
	// "log"
	"net/http"
	"os"
	// "path/filepath"
	"time"

	"bytes"
	"math/rand"
	"os/signal"

	"../../scv"
)

func main() {

	rand.Seed(time.Now().UTC().UnixNano())

	app := scv.NewApplication("vspg11")
	req := func(token string, jsonData string) {
		time.Sleep(3 * time.Second)
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
	go app.Run()

	os.RemoveAll(app.Name + "_data")

}
