package main

import (
	"bufio"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
	"sync"

//	"os"
)


// Simple serial check of getting and setting
func TestTCPSimple(t *testing.T) {
	go serverMain()
        time.Sleep(1 * time.Second) // one second is enough time for the server to start
	
	name := "hi.txt"
	contents := "bye"
	exptime := 300000
 	conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}

	scanner := bufio.NewScanner(conn)
	
	// Write a file
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp := scanner.Text() // extract the text from the buffer
	//t.Error(resp)
	arr := strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	version, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
//t.Error("read started here-----------------------------")
	fmt.Fprintf(conn, "read %v\r\n", name) // try a read now
	scanner.Scan()
    resp = scanner.Text() // extract the text from the buffer
	//t.Error(resp)
	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) // expect only accepts strings, convert int version to string
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	scanner.Scan()
	expect(t, contents, scanner.Text())
fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", name, len(contents), exptime, contents)
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	//t.Error(resp)
	arr = strings.Split(resp, " ") // split into OK and <version>
	if arr[0] != "OK"{
	t.Error("OK EXPECTED")
	}
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
//t.Error("read started here-----------------------------")
	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name,version,len(contents), exptime, contents)

	scanner.Scan()
    resp = scanner.Text() // extract the text from the buffer
	//t.Error(resp)
	arr = strings.Split(resp, " ") // split into OK and <version>

	if arr[0] != "OK" {
	t.Error("OK EXPECTED")
	}
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}

	var wg sync.WaitGroup

	for i:=0;i<10;i++{

    wg.Add(1)
	go concurrency_check(t,conn,&wg)    
   }
    wg.Wait()
	
}

func concurrency_check(t *testing.T,conn net.Conn,wg *sync.WaitGroup) {
	data:="this is going to test concurrent write to the same file key"
	newscanner := bufio.NewScanner(conn)
	fmt.Fprintf(conn, "write %v %v %v\r\n%v\r\n", "t.txt", len(data),3000,data)
	newscanner.Scan() // read first line
	resp := newscanner.Text() // extract the text from the buffer
	//t.Error(resp)
	arr := strings.Split(resp, " ") // split into OK and <version>
	//t.Error("failed")
	expect(t, arr[0], "OK")
	_, err := strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	wg.Done()

}

// Useful testing function
func expect(t *testing.T, a string, b string) {
	if a != b {
		t.Error(fmt.Sprintf("Expected %v, found %v", b, a)) // t.Error is visible when running `go test -verbose`
	}
}
