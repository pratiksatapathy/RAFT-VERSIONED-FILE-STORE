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
	
	 conn, err := net.Dial("tcp", "localhost:8080")
	if err != nil {
		t.Error(err.Error()) // report error through testing framework
	}
	name := "b1.txt"
	contents := "bye"
	exptime := 30000
 	
	scanner := bufio.NewScanner(conn)

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
	
	
	//test no 2---------read the same file

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
	
	
	//test no 3----------write to same file again to update version no

	fmt.Fprintf(conn, "write %v %v\r\n%v\r\n", name, len(contents),  contents)
	scanner.Scan() 
	resp = scanner.Text() 
	arr = strings.Split(resp, " ") 
	if arr[0] != "OK"{
	t.Error("OK EXPECTED")
	}
	versionNew := version  //keeping the old value
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	if version == versionNew{
		t.Error("same version found after update")
	}

	//changing the data to be saved in the file
	contents = "this is the new content"

	//test no 4----------cas to same file again to update version no without any expire time

	fmt.Fprintf(conn, "cas %v %v %v\r\n%v\r\n", name,version,len(contents),contents)

	scanner.Scan()
    resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>

	expect(t,arr[0],"OK")
	
	versionNew = version  //
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	if version == versionNew{
		t.Error("same version found after update")
	}

	//test no 4----------cas to same file again to update version no with expire timme

	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name,version,len(contents),exptime,contents)

	scanner.Scan()
    resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>

	expect(t,arr[0],"OK")
	
	versionNew = version  //
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	if version == versionNew{
		t.Error("same version found after update")
	}

	///test  

	fmt.Fprintf(conn, "cas %v %v %v %v\r\n%v\r\n", name,version+45,len(contents),exptime,contents)

	scanner.Scan()
    resp = scanner.Text() // extract the text from the buffer
	arr = strings.Split(resp, " ") // split into OK and <version>

	expect(t,arr[0],"ERR_VERSION")
	
	versionNew = version  //
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	if version != versionNew{
		t.Error("version should be the last updated one")
	}


	//test no 5---------read the same file after the cas update to check that new content is available

	fmt.Fprintf(conn, "read %v\r\n", name) 
	scanner.Scan()
    resp = scanner.Text() 
	arr = strings.Split(scanner.Text(), " ")
	expect(t, arr[0], "CONTENTS")
	expect(t, arr[1], fmt.Sprintf("%v", version)) 
	expect(t, arr[2], fmt.Sprintf("%v", len(contents)))	
	//contents = "this is different new content"

	scanner.Scan()
	expect(t, contents, scanner.Text())

	//test no 6---------deleting the old file and checking the returned version number

	fmt.Fprintf(conn, "delete %v\r\n", name) 
	scanner.Scan() // read first line
	resp = scanner.Text() // extract the text from the buffer
	//t.Error(resp)
	arr = strings.Split(resp, " ") // split into OK and <version>
	expect(t, arr[0], "OK")
	versionNew = version  //
	version, err = strconv.ParseInt(arr[1], 10, 64) // parse version as number
	if err != nil {
		t.Error("Non-numeric version found")
	}
	if version != versionNew{
		t.Error("deleted version should be same as the last write returned version")
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
