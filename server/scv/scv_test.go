package scv

import (
	//"time"
	// "sync"
	"testing"
	// "fmt"
	"net/http"
	"net/http/httptest"

	"github.com/stretchr/testify/assert"
)

var serverAddr string = "http://127.0.0.1/streams/wowsogood"

func TestPostStream(t *testing.T) {
	app := NewApplication("testServer")
	req, _ := http.NewRequest("POST", serverAddr, nil)
	w := httptest.NewRecorder()
	app.PostStreamHandler()(w, req)
	assert.Equal(t, w.Code, 200)
}
