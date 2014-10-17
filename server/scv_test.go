package scv

import (
	//"time"
	"../util"
	"sync"
	"testing"
	"time"
	// "fmt"
	"net/http"
	"net/http/httptest"

	"github.com/stretchr/testify/assert"
)

var serverAddr := "http://127.0.0.1/streams/wowsogood"

func TestPostStream(t *testing.T) {

	req, nil := http.NewRequest("POST", "http://example/foo", nil)
	w := httptest.NewRecorder()


}
