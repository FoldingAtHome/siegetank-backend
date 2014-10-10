/*
Hi,

I'm fairly new to Go and I'm trying to get the pattern right for a channel-based service.

I have REST Endpoints in the form of /modify and /die for some resource on a server. I'd like the behavior to be such that:

GET /modify -> OK
GET /modify -> OK
GET /die -> OK
GET /modify -> error 

In addition, trying to modify my resource after dieing is erroneous. Another example:

GET /modify -> OK
GET /die -> OK
GET /die -> error
GET /modify -> error


func deferAndFinish(funcs []Callable) {
    var wg sync.WaitGroup
    wg.Add(len(funcs))
    for i := 0; i < len(funcs); i++ {
        go func(i int) {
            funcs[i]()
            wg.Done()
        }(i)
    }
    wg.Wait()
}

type Callable func()

func t1() {
    serv := NewServer()
    deferAndFinish([]Callable{
        func() {serv.work("Cancer")},
        func() {serv.work("Ebola")},
        func() {serv.die()},
        })
}

*/

package main

import(
    "fmt"
    "time"
    "sync"
    "sync/atomic"
    "math/rand"
    "runtime"
)

type Activation struct {
    token string
    engine string
    stream_chan chan string
}

type Server struct {
    quit chan bool
    task chan string
    task2 chan Activation
    
    dead_waitgroup sync.WaitGroup
    dead_signal int32
}

func NewServer() *Server {
    s := &Server{
        quit: make(chan bool), 
        task: make(chan string),
        task2: make(chan Activation),
        dead_signal: 0,
    }
    s.dead_waitgroup.Add(1)
    go func() {
        s.run()
        s.dead_waitgroup.Done()
        }()
    return s
}

// returns iff run() has completed
func (s *Server) die() {
    if atomic.CompareAndSwapInt32(&s.dead_signal, 0, 1) {
        close(s.quit)
        // wait for run() to finish
        s.dead_waitgroup.Wait()
    }
}

func (s *Server) work(data string) {
    select {
        case <- s.quit:
        case s.task <- data:
    }
}

func (s *Server) activate(token, engine string) {
    act := Activation{token, engine, make(chan string)}
    select {
        case <- s.quit:
        case s.task2 <- act:
            fmt.Println("Activated stream", <- act.stream_chan)
    }
}

// This method is safe. Once quit has been closed, then no work can continue.
func (s *Server) run() {
    for {
        select {
            case <- s.quit:
                fmt.Println("run finished")
                return
            case data := <- s.task:
                time.Sleep(time.Millisecond)
                fmt.Println(data)
            case act := <- s.task2:
                time.Sleep(time.Millisecond)
                act.stream_chan <- act.token
        }
    }
}

func test() {
    serv := NewServer()
    var wg sync.WaitGroup
    wg.Add(5)
    go func() {
        //serv.work("Cancer")
        wg.Done()
    }()
    go func() {
        // serv.work("Flu")
        serv.activate("2345", "openmm2")
        wg.Done()
    }()
    go func() {
        serv.activate("1234", "openmm")
        wg.Done()
    }()
    go func() {
        time.Sleep(time.Millisecond*time.Duration(rand.Intn(10)))
        serv.die()
        wg.Done()
    }()
    go func() {
    //    serv.die()
        wg.Done()
    }()
    wg.Wait()
}

func main() {
    fmt.Println("Old Procs:", runtime.GOMAXPROCS(2))
    test()
    for i := 0; i < 200; i++ {
        fmt.Println(i)
        test()
        // time.Sleep(time.Second)
    }
}