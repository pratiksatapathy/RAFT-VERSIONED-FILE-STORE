package main

import (
    "fmt"
    "net"
    "os"
)

var fileMap map[string]FileStore

//structure to be stored in the map
type FileStore struct {
fileContent [2000]byte
file_version int64
expire_time int16
}


func main() {
  serverMain()
}
func serverMain(){

	//initialize the map variable
	fileMap = make(map[string]FileStore)
    listenerVar, errVar := net.Listen("tcp", "localhost"+":"+"8080")
    if errVar != nil {
        fmt.Println("Error occured:", errVar.Error());os.Exit(1)
    }

    defer listenerVar.Close() //delayed close
    fmt.Println("server started at port 8080")
    for {//infinite loop
        
        connVar, errVar := listenerVar.Accept()
        if errVar != nil {
            fmt.Println("Could not establish connection: ", errVar.Error())
            os.Exit(1)
        }
	//new thread for connnections
        go spawnRequestHandler(connVar)
    }
}


func spawnRequestHandler(conn net.Conn) {

  buf := make([]byte, 10)
  reqLen, errVar := conn.Read(buf)
  fmt.Println(reqLen)
	  fmt.Println(buf)
  if errVar != nil {
    fmt.Println("Error reading:", errVar.Error())
  }
  conn.Write(buf) // print back what is sent from client for now .....
/*

to perform parsing and storing of the file in a map

file name as the key and other parameters in the structure

*/

  conn.Close()
}
