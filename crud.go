package main

import (
    "fmt"
    "sync"
    "time"
    "math/rand"
)

var letters = []rune("012345689ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz")

func randSeq(n int) string {
    b := make([]rune, n)
    for i := range b {
        b[i] = letters[rand.Intn(len(letters))]
    }
    return string(b)
}

// What is the proper, idiomatic way of expressing asynchronous CRUD (CREATE, READ, UPDATE, DELETE) semantics in Go?

// A manager manages a set of employees.
type Manager struct {
    sync.RWMutex
    active_employees map[string]EmployeeProp
    inactive_employees map[string]EmployeeProp
    expirations chan string
}

type EmployeeProp struct {
    job string
    age int    
    timer *time.Timer
}

var manager = &Manager{active_employees: make(map[string]EmployeeProp),
                       inactive_employees: make(map[string]EmployeeProp),
                       expirations: make(chan string)}

// My REST APIs are:
// POST /employees/:employee_name (CREATE)
// GET /employees/:employee_name (READ)
// PUT /employees/:employee_name (UPDATE)
// DELETE /employees/:employee_name (DELETE)
// These handlers are all handled inside goroutines, asynchronously

func (m *Manager) AddEmployee(name string, prop EmployeeProp) {
    m.Lock()
    fmt.Println("Adding employee", name)
    m.inactive_employees[name] = prop
    m.Unlock()
}

func (m *Manager) RemoveEmployee(name string) {
    m.Lock()
    fmt.Println("Removing employee", name)    
    delete(m.inactive_employees, name)
    delete(m.active_employees, name)
    m.Unlock()
}

func (m *Manager) ActivateEmployee(name string) {
    m.Lock()
    fmt.Println("Activating Employee", name)
    prop, ok := m.inactive_employees[name]
    if ok {
        prop.timer = time.AfterFunc(time.Duration(rand.Intn(2000))*time.Millisecond, func() {
                m.expirations <- name
            })
        m.active_employees[name] = prop
        delete(m.inactive_employees, name)
    }
    m.Unlock()
}

func (m *Manager) DeactivateEmployee(name string) {
    m.Lock()
    fmt.Println("Deactivating Employee", name)
    prop, ok := m.active_employees[name]
    if ok {
        m.inactive_employees[name] = prop
        delete(m.active_employees, name) 
    }
    m.Unlock()
}

// Listen for expirations
func (m* Manager) Init() {
    for {
        select {
        case employee_name, ok := <- m.expirations:
            if ok {
                m.DeactivateEmployee(employee_name) 
            } else {
                fmt.Println("Shutting down")
                return
            }
        }
    }
}

func (m* Manager) Cleanup() {
    close(m.expirations)
}

func main() {

    go manager.Init()

    for i:= 0; i < 10; i++ {
        name := randSeq(5)
        go manager.AddEmployee(name, EmployeeProp{"engineer", 44, nil})
        go manager.ActivateEmployee(name)
        // go manager.DeactivateEmployee(name)
        // go manager.RemoveEmployee(name)
    }

    var input string
    fmt.Scanln(&input)
    manager.Cleanup()
    manager = nil
    time.Sleep(time.Duration(5)*time.Second)
}