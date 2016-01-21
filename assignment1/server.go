package main

import (
	"fmt"
	"net"
	"os"
//	"unicode/utf8"
	"bufio"
	"bytes"
	"strings"
	"strconv"
	"io/ioutil"
	"time"

	"io"
	"sync"
)
const (
	WRITE = "write"
	CAS = "cas"
	READ = "read"
	DELETE = "delete"

)
type fileAttr struct {
	filestatus bool
	file_ver string
    file_creation_time string
	noofbytes string
	file_expire string
	filedata string
}
var MutX *sync.Mutex
func main() {
	serverMain()
}
func serverMain() {

	listenerVar, errVar := net.Listen("tcp", "localhost" + ":" + "8080")
	if errVar != nil {
		fmt.Println("Error occured:", errVar.Error()); os.Exit(1)
	}
	MutX = &sync.Mutex{}

	//defer listenerVar.Close() //delayed close
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

func readFromConnection(conn net.Conn) (data []byte) {
	large_buf := make([]byte, 0, 4096)
	small_buf := make([]byte, 256)
	for {
		conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		count, err := conn.Read(small_buf)
		if err != nil {
			if err != io.EOF {
				fmt.Println("read error:", err)
				return []byte("")
			}
			break
		}
		large_buf = append(large_buf, small_buf[:count]...)
		if count < 256 {
			break
		}
	}
	data = large_buf
	return data
}
func spawnRequestHandler(conn net.Conn) {

	dataMaster := readFromConnection(conn)

	//parsing
	for {


		outbounddata, unprocessd := parseIncomingData(dataMaster, conn)
		fmt.Fprintf(conn, string(outbounddata))
		fmt.Println("outbound data: " + string(outbounddata))

		unprocessd = append(unprocessd, readFromConnection(conn)...)
		if string(unprocessd[:2]) == "\r\n" && len(unprocessd) > 2 {
			dataMaster = unprocessd[2:]

		}else {
			if string(unprocessd[:2]) == "\r\n" && len(unprocessd) == 2 {
				unprocessd = unprocessd[:0]
				fmt.Println("connection closed")
				conn.Close()
			}else {
				conn.Write([]byte("ERR_INTERNAL\r\n"))
				fmt.Println("connection closed")
				conn.Close()
			}

			break
		}

	}
	//conn.Close()
}
func parseIncomingData(dataStream []byte, conn net.Conn) (response []byte, unprocessed []byte) {

	//	var stringAccumulator bytes.Buffer
	readbuffer := bytes.NewBuffer(dataStream)
	reader := bufio.NewReader(readbuffer)
	buffer, err := reader.ReadBytes('\r') //reading the firstline of command
	check(err)
	data := strings.Trim(string(buffer), " ")
	data = strings.TrimRight(string(buffer), "\r")
	var dataarray []string
	dataarray = strings.Split(data, " ")

	if dataarray[0] == WRITE { //-----------------------------write action---------------------
		dataStream = dataStream[len(buffer) + 1:] //remove the firstline
		response, dataStream = writeCommandProcessing(dataarray, dataStream)


	}  else if dataarray[0] == CAS {
		dataStream = dataStream[len(buffer) + 1:] //remove the firstline
		response, dataStream = casCommandProcessing(dataarray, dataStream)


	}else if dataarray[0] == READ || dataarray[0] == DELETE {
		dataStream = dataStream[len(buffer) - 1:]
		response = readCommandProcessing(dataarray)

	}  else {
		response = []byte("ERR_CMD_ERR\r\n")

	}

	return response, dataStream
}

func readFileFromStore(filename string) (filestatus bool, file_ver string, file_creation_time string, noofbytes string, file_expire string, filedata string) {
	fileContent, e := synchronisedFIleHandling(filename, []byte(""), true) //read request:true

	if e != nil {//file not found condition
		fmt.Println(e.Error())
		return false, "", "", "", "", ""
	}

	var splitarray []string
	splitarray = strings.Split(string(fileContent), "\n")
	storeddata := strings.Join(splitarray[4:], "\n")
	exp_dur, _ := strconv.Atoi(splitarray[3])
	newxpry_time := "-1"
	if exp_dur == -1 {
		filestatus = true
	}else {
		createdTime := splitarray[1]
		ct, _ := time.Parse(time.RFC3339, createdTime)
		t := (time.Now().UnixNano()) - ct.UnixNano()
		t = t / int64(time.Second)
		if t < (int64(exp_dur)) {
			//valid
			filestatus = true
			file_expire = strconv.Itoa(int(t))
		}else {
			//invalid
			filestatus = false
		}

		newxpry_time = strconv.FormatInt((int64(exp_dur) - t), 10)
	}

	return filestatus, splitarray[0], splitarray[1], splitarray[2], newxpry_time, storeddata
}
func writeCommandProcessing(dataarray []string, datastream []byte) (response []byte, dataStream []byte) {

	if len(dataarray) < 3 {
		return []byte("ERR_CMD_ERR"), []byte("\r\n")
	}
	var actiontype, filename, bytecount, expiretime string
	if len(dataarray) == 3 {

		actiontype = dataarray[0]
		actiontype = actiontype
		filename = dataarray[1]
		bytecount = dataarray[2]
		expiretime = "-1"

	}
	if len(dataarray) == 4 {

		actiontype = dataarray[0]
		filename = dataarray[1]
		bytecount = dataarray[2]
		expiretime = dataarray[3]
	}

	totalbytes, _ := strconv.Atoi(bytecount)
	dataStream = datastream
	if len(dataStream) < (totalbytes + 2) {
		return []byte("ERR_CMD_ERR\r\n"), []byte("\r\n")
	}
	childdataStream := dataStream[:totalbytes]
	dataStream = dataStream[totalbytes:]//adding increment to adjust for \r\n

	//read existing file system


	response,_ = criticalSectionBlockForWRTnRD("NA", filename, totalbytes, expiretime, childdataStream)
	//fmt.Println(string(dataStream))
	return response, dataStream

}

func criticalReadWrite(filename string, totalbytes int, expiretime string, childdataStream []byte) (response []byte) {
	rfilevalid, rfile_Ver, r_fileCreationTime, _, r_fileExpiry, _ := readFileFromStore(filename)
	r_fileExpiry = r_fileExpiry
	r_fileCreationTime = r_fileCreationTime

	if rfilevalid == true {
		var newVer int
		newVer, err := strconv.Atoi(rfile_Ver)
		check(err)

		newVer = newVer + 1

		rfile_Ver = strconv.Itoa(newVer)
	}else {
		rfile_Ver = strconv.Itoa(1) //intial version of the file

	}
	writable := []byte(rfile_Ver + "\n" + time.Now().Format(time.RFC3339) + "\n" + strconv.Itoa(totalbytes) + "\n" + expiretime + "\n" + string(childdataStream))//import "time"
	//fmt.Print(string(writable))
	_, e := synchronisedFIleHandling(filename, writable, false) //write request
	check(e)
	response = []byte("OK " + rfile_Ver + "\r\n")
	return response
}
func criticalSectionBlockForWRTnRD(vers string, filename string, totalbytes int, expiretime string, childdataStream []byte) (response []byte,fileattrStuc fileAttr) {
	fmt.Println(vers+" --------- "+filename)
	MutX.Lock()
	fmt.Print("entered \n")

	if vers == "NA" { //NOT APPLICABLE IN WRITE
		response = criticalReadWrite(filename, totalbytes, expiretime, childdataStream)

	}else if vers == "RO" { //READ ONLY SO
		rdfilevalid, rdfile_Ver, rd_fileCreationTime, rd_noofbytes, rd_fileExpiry, rd_fileData := readFileFromStore(filename)
		fileattrStuc.file_creation_time = rd_fileCreationTime
		fileattrStuc.filestatus = rdfilevalid
		fileattrStuc.file_ver = rdfile_Ver
		fileattrStuc.noofbytes = rd_noofbytes
		fileattrStuc.file_expire = rd_fileExpiry
		fileattrStuc.filedata = rd_fileData


	}else if vers == "DL" {
		err := os.Remove(filename)
		check(err)

	}else {
		response = casCriticalRW(vers, filename, totalbytes, expiretime, childdataStream)
	}
	fmt.Print("exited \n")
	MutX.Unlock()
	return response,fileattrStuc
}
func casCriticalRW(vers string, filename string, totalbytes int, expiretime string, childdataStream []byte) (response []byte) {

	//read existing file system

	rfilevalid, rfile_Ver, r_fileCreationTime, _, r_fileExpiry, _ := readFileFromStore(filename)
	r_fileExpiry = r_fileExpiry
	r_fileCreationTime = r_fileCreationTime

	if rfilevalid == true {
		var newVer int
		newVer, err := strconv.Atoi(rfile_Ver)
		check(err)

		if vers == rfile_Ver {
			newVer = newVer + 1

			rfile_Ver = strconv.Itoa(newVer)
			writable := []byte(rfile_Ver + "\n" + time.Now().Format(time.RFC3339) + "\n" + strconv.Itoa(totalbytes) + "\n" + expiretime + "\n" + string(childdataStream))//import "time"
			//fmt.Print(string(writable))
			_, e := synchronisedFIleHandling(filename, writable, false) //write request
			check(e)
			response = []byte("OK " + rfile_Ver + "\r\n")
		}else {
			response = []byte("ERR_VERSION " + rfile_Ver + "\r\n")
		}
	}else {
		response = []byte("ERR_FILE_NOT_FOUND\r\n")
	}
	return response
}
func synchronisedFIleHandling(filename string, data []byte, isRead bool) (fileContent []byte, e error) {

	fileContent = []byte("")

	if isRead == true {
		fileContent, e = readFile(filename)
	}else {
		writeToFile(filename, data)
	}
	return fileContent, e

}
func readFile(filename string) (fileContent []byte, e error) {
	fileContent, e = ioutil.ReadFile(filename)
	fmt.Println(string(fileContent))
	//check(e)
	return fileContent, e

}
func writeToFile(filename string, data []byte) {
	err := ioutil.WriteFile(filename, data, 0644)

	check(err)

}
func casCommandProcessing(dataarray []string, datastream []byte) (response []byte, dataStream []byte) {
	if len(dataarray) < 4 {
		//fmt.Println("Incorrect format")
		return []byte("ERR_MD_ERR"), []byte("\r\n")

	}
	var filename, bytecount, expiretime, vers string
	if len(dataarray) == 4 {

		filename = dataarray[1]; vers = dataarray[2]
		bytecount = dataarray[3]; expiretime = "-1"
	}
	if len(dataarray) == 5 {

		filename = dataarray[1]; vers = dataarray[2]
		bytecount = dataarray[3]; expiretime = dataarray[4]
	}

	totalbytes, _ := strconv.Atoi(bytecount)
	dataStream = datastream
	if len(dataStream) < (totalbytes + 2) {
		return []byte("ERR_CMD_ERR\r\n"), []byte("\r\n")
	}
	childdataStream := dataStream[:totalbytes]
	dataStream = dataStream[totalbytes:]//adding increment to adjust for \r\n

	response,_ = criticalSectionBlockForWRTnRD(vers, filename, totalbytes, expiretime, childdataStream)
	//fmt.Println(string(dataStream))
	return response, dataStream

}
func fileRemove(filename string){
	err := os.Remove(filename)
	check(err)
}
func readCommandProcessing(dataarray []string) (response []byte) {
	//read processing

	filename := dataarray[1]
	var fileattrStruct fileAttr
	response,fileattrStruct = criticalSectionBlockForWRTnRD("RO", filename, 0,"",[]byte(""))

	//rdfilevalid, rdfile_Ver, rd_fileCreationTime, rd_noofbytes, rd_fileExpiry, rd_fileData := readFileFromStore(filename)

	if fileattrStruct.filestatus == true {
		if dataarray[0] == DELETE {
			criticalSectionBlockForWRTnRD("DL", filename, 0,"",[]byte(""))

			response = []byte("OK " + fileattrStruct.file_ver + "\r\n")

		}else {
			if fileattrStruct.file_expire == "-1" {
				response = []byte("CONTENTS " + fileattrStruct.file_ver + " " + fileattrStruct.noofbytes + "\r\n" + fileattrStruct.filedata + "\r\n")

			}else {
				response = []byte("CONTENTS " + fileattrStruct.file_ver + " " + fileattrStruct.noofbytes + " " + fileattrStruct.file_expire + "\r\n" + fileattrStruct.filedata+ "\r\n")
			}

		}
	}else {
		response = []byte("ERR_FILE_NOT_FOUND\r\n")
	}
	return response
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}


