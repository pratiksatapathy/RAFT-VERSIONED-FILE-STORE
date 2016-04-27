package main

import (
	"github.com/pratiksatapathy/cs733/assignment4/raftnode"
	"testing"
	"time"
	"fmt"
	"net"
	"errors"
	"bufio"
	"strings"
	"strconv"
	"bytes"
	"os"
	"encoding/json"
	"io/ioutil"
	"os/exec"
	"math/rand"
	_ "sync"


	"sync"
)

//var Rafts []raftnode.RaftNode
//var mck *mock.MockCluster
//func init() {
//
//	startRaft()
//}

//starting rafter
var netconfigs []raftnode.NetConfig
var rafts [6]*exec.Cmd
//cleaning logs before one more test instance starts

func init() {

	os.RemoveAll("dir1"); os.RemoveAll("dir2"); os.RemoveAll("dir3"); os.RemoveAll("dir4"); os.RemoveAll("dir5")

	//var netconfigs []raftnode.NetConfig

	netconfigs = []raftnode.NetConfig{raftnode.NetConfig{Id:1, Host:"localhost", Port:8080},
		raftnode.NetConfig{Id:2, Host:"localhost", Port:8081},
		raftnode.NetConfig{Id:3, Host:"localhost", Port:8082},
		raftnode.NetConfig{Id:4, Host:"localhost", Port:8083},
		raftnode.NetConfig{Id:5, Host:"localhost", Port:8084}}

	var configs []raftnode.Config

	configs = []raftnode.Config{raftnode.Config{Cluster:netconfigs, Id:1, LogDir:"dir1", ElectionTimeout:3900, HeartbeatTimeout:500},
		raftnode.Config{Cluster:netconfigs, Id:2, LogDir:"dir2", ElectionTimeout:4700, HeartbeatTimeout:500},
		raftnode.Config{Cluster:netconfigs, Id:3, LogDir:"dir3", ElectionTimeout:2500, HeartbeatTimeout:500},
		raftnode.Config{Cluster:netconfigs, Id:4, LogDir:"dir4", ElectionTimeout:4900, HeartbeatTimeout:500},
		raftnode.Config{Cluster:netconfigs, Id:5, LogDir:"dir5", ElectionTimeout:4400, HeartbeatTimeout:500}}


	data_configs, _ := json.Marshal(configs)
	er := ioutil.WriteFile("configs.json", data_configs, 0644)
	check(er)



	for i := 1; i < 6; i++ {
		param := strconv.Itoa(i)
		rafts[i] = exec.Command("./rafte", param)
		rafts[i].Stdout = os.Stdout
		rafts[i].Stdin = os.Stdin
		fmt.Println("***fileservers up***")

		rafts[i].Start()
	}
	time.Sleep(10 * time.Second)
}
func startIndex(i int){

	param := strconv.Itoa(i)
	rafts[i] = exec.Command("./rafte", param)
	rafts[i].Stdout = os.Stdout
	rafts[i].Stdin = os.Stdin
	fmt.Println("***fileservers up***")

	rafts[i].Start()
	time.Sleep(5 * time.Second)
}
func cleanup() {

	for i := 1; i < 6; i++ {

		rafts[i].Process.Kill()
	}
	fmt.Println("***fileservers down***")
}
//func TestOne(t *testing.T) {
//
//	cl := mkClient(t, "localhost:8081")
//
//	m, err := rRead(cl, "c733net")
//
//	fmt.Println("out :", m)
//	check(err)
//
//	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)
//
//	cl.close()
//	cleanup()
//
//}

//
//
func TestRPC_leaderShutDown(t *testing.T) {


	cle := mkClient(t, "localhost:8080")
	c := &ClientContainer{cl:cle}
	//defer c.cl.close()


	// Write file cs733net
	data := "Cloud fun"
	m, err := c.rWrite("somefilea", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// CAS in new value

	data = "Cloud fun 2"
	m, err = c.rWrite("somefilea", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	rafts[3].Process.Kill()

	time.Sleep( 10 * time.Second)
	fmt.Println("----------killd----------")
	cle = mkClient(t, "localhost:8080")
	c = &ClientContainer{cl:cle}


	data = "Cloud fun 3"

	// Cas new value
	m, err = c.rWrite("somefilea", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	data = "Cloud fun 4"

	// Cas new value
	m, err = c.rWrite("somefilea", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	//startIndex(1) //start node 1

	//rafts[3].Process.Kill() //kill node 3 so that node 1 becomes leader again


	//reading files at node 1 to see if it has recovered succesfully
	cle = mkClient(t, "localhost:8080")
	c = &ClientContainer{cl:cle}
	m, _ = c.rRead("somefilea")
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 4")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
	}

	// delete
	m, err = c.rDelete("somefilea")
	expect(t, m, &Msg{Kind: 'O'}, "delete success", err)
	time.Sleep(2 * time.Second) //giving some time to kill

//	rafts[3].Process.Kill() //kill node 3 so that node 1 becomes leader again
//	time.Sleep(10 * time.Second) //give some time for killing this node and election
	//startIndex(3);



}

func TestRPC_BasicSequential(t *testing.T) {


	cle := mkClient(t, "localhost:8080")
	c := &ClientContainer{cl:cle}

	// Read non-existent file cs733net


	m, err := c.rRead( "cs733net")
	fmt.Println("=",m.Filename,string(m.Contents))
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)
	//fmt.Println(cl.conn.LocalAddr().String())


	// Read non-existent file cs733net
	m, err = c.rDelete("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)


	// Write file cs733net
	data := "Cloud fun"
	m, err = c.rWrite("cs733net", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = c.rRead("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

	// CAS in new value
	version1 := m.Version
	data2 := "Cloud fun 2"
	// Cas new value
	m, err = c.rCAS("cs733net", version1, data2, 0)
	expect(t, m, &Msg{Kind: 'O'}, "cas success", err)

	// Expect to read it back
	m, err = c.rRead("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "read my cas", err)

	// Expect Cas to fail with old version
	m, err = c.rCAS("cs733net", version1, data, 0)
	expect(t, m, &Msg{Kind: 'V'}, "cas version mismatch", err)

	// Expect a failed cas to not have succeeded. Read should return data2.
	m, err = c.rRead("cs733net")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data2)}, "failed cas to not have succeeded", err)

	// delete
	m, err = c.rDelete("cs733net")
	expect(t, m, &Msg{Kind: 'O'}, "delete success", err)

	// Expect to not find the file
	m, err = c.rRead("cs733net")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	c.cl.close()
}



func TestRPC_Binary(t *testing.T) {

	cle := mkClient(t, "localhost:8080")
	c := &ClientContainer{cl:cle}


	// Write binary contents
	data := "\x00\x01\r\n\x03" // some non-ascii, some crlf chars
	m, err := c.rWrite("binfile", data, 0)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back
	m, err = c.rRead("binfile")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(data)}, "read my write", err)

	c.cl.close()

}
func TestRPC_Chunks(t *testing.T) {
	// Should be able to accept a few bytes at a time
	cl := mkClient(t, "localhost:8080")
	defer cl.close()
	var err error
	snd := func(chunk string) {
		if err == nil {
			err = cl.send(chunk)
		}
	}

	// Send the command "write teststream 10\r\nabcdefghij\r\n" in multiple chunks
	// Nagle's algorithm is disabled on a write, so the server should get these in separate TCP packets.
	snd("wr")
	time.Sleep(10 * time.Millisecond)
	snd("ite test")
	time.Sleep(10 * time.Millisecond)
	snd("stream 1")
	time.Sleep(10 * time.Millisecond)
	snd("0\r\nabcdefghij\r")
	time.Sleep(10 * time.Millisecond)
	snd("\n")
	var m *Msg
	m, err = cl.rcv()
	expect(t, m, &Msg{Kind: 'O'}, "writing in chunks should work", err)
}
func TestRPC_Batch(t *testing.T) {
	// Send multiple commands in one batch, expect multiple responses
	cl := mkClient(t, "localhost:8080")
	defer cl.close()
	cmds := "write batch1 3\r\nabc\r\n" +
	"write batch2 4\r\ndefg\r\n" +
	"read batch1\r\n"

	cl.send(cmds)
	m, err := cl.rcv()
	expect(t, m, &Msg{Kind: 'O'}, "write batch1 success", err)
	m, err = cl.rcv()
	expect(t, m, &Msg{Kind: 'O'}, "write batch2 success", err)
	m, err = cl.rcv()
	expect(t, m, &Msg{Kind: 'C', Contents: []byte("abc")}, "read batch1", err)
}

func TestRPC_BasicTimer(t *testing.T) {
	cle := mkClient(t, "localhost:8080")
	c := &ClientContainer{cl:cle}

	// Write file cs733, with expiry time of 3 seconds
	str := "Cloud fun"
	m, err := c.rWrite("cs733", str, 3)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

	// Expect to read it back immediately.

	m, err = c.rRead("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "read my cas", err)

	time.Sleep(3 * time.Second)

	// Expect to not find the file after expiry
	m, err = c.rRead("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found", err)

	// Recreate the file with expiry time of 1 second
	m, err = c.rWrite("cs733", str, 1)
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	// Overwrite the file with expiry time of 5. This should be the new time.
	m, err = c.rWrite("cs733", str, 5)
	expect(t, m, &Msg{Kind: 'O'}, "file overwriten with exptime=4", err)

	// The last expiry time was 3 seconds. We should expect the file to still be around 2 seconds later
	time.Sleep(2 * time.Second)

	// Expect the file to not have expired.
	m, err = c.rRead("cs733")
	expect(t, m, &Msg{Kind: 'C', Contents: []byte(str)}, "file to not expire until 4 sec", err)

	time.Sleep(3 * time.Second)
	// 5 seconds since the last write. Expect the file to have expired
	m, err = c.rRead("cs733")
	expect(t, m, &Msg{Kind: 'F'}, "file not found after 4 sec", err)

	// Create the file with an expiry time of 1 sec. We're going to delete it
	// then immediately create it. The new file better not get deleted.
	m, err = c.rWrite("cs733", str, 3)
	expect(t, m, &Msg{Kind: 'O'}, "file created for delete", err)

	m, err = c.rDelete("cs733")
	expect(t, m, &Msg{Kind: 'O'}, "deleted ok", err)

	m, err = c.rWrite("cs733", str, 0) // No expiry
	expect(t, m, &Msg{Kind: 'O'}, "file recreated", err)

	time.Sleep(1100 * time.Millisecond) // A little more than 1 sec
	m, err = c.rRead("cs733")
	expect(t, m, &Msg{Kind: 'C'}, "file should not be deleted", err)
	//
}

// nclients write to the same file. At the end the file should be
// any one clients' last write

func TestRPC_ConcurrentWrites(t *testing.T) {
	startIndex(3)
	nclients := 10
	niters := 10
	clientContainers := make([]*ClientContainer, nclients)
	var cli *Client
	for i := 0; i < nclients; i++ {
		cli = mkClient(t, "localhost:8080")

		if cli == nil {
			t.Fatalf("Unable to create client #%d", i)
		}

		clientContainers[i] = &ClientContainer{cl:cli}

	}

	errCh := make(chan error, nclients)
	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to begin concurrently
	sem.Add(1)
	ch := make(chan *Msg, nclients * niters) // channel for all replies
	for i := 0; i < nclients; i++ {
		//fmt.Println()
		go func(i int, cl *ClientContainer) {
			sem.Wait()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				//fmt.Print(". ")
				m, err := cl.rWrite("concWrite", str, 0)
				if err != nil {
					errCh <- err//
					break
				} else {
					ch <- m
				}
			}
		}(i, clientContainers[i])
	}
	time.Sleep(5000 * time.Millisecond) // give goroutines a chance

	sem.Done()                         // Go!
	// There should be no errors
	for i := 0; i < nclients * niters; i++ {
		select {
		case m := <-ch:
			if m.Kind != 'O' {
				t.Fatalf("Concurrent write failed with kind=%c", m.Kind)
			}
		case err := <-errCh:
			t.Fatal(err)
		}
	}

	m, _ := clientContainers[0].rRead("concWrite")
	// Ensure the contents are of the form "cl <i> 9"
	// The last write of any client ends with " 9"
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 9")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg = %v", string(m.Contents))
	}

	for i := 0; i < nclients; i++ {
		clientContainers[i].cl.close()
	}
}

func TestRPC_ConcurrentCas(t *testing.T) {
	//startIndex(3);
	nclients := 4
	niters := 4

	clientContainers := make([]*ClientContainer, nclients)
	var cli *Client
	for i := 0; i < nclients; i++ {
		cli = mkClient(t, "localhost:8080")

		if cli == nil {
			t.Fatalf("Unable to create client #%d", i)
		}

		clientContainers[i] = &ClientContainer{cl:cli}
	}


	var sem sync.WaitGroup // Used as a semaphore to coordinate goroutines to *begin* concurrently
	sem.Add(1)

	m, _ := clientContainers[0].rWrite( "concCas", "first", 0)
	ver := m.Version
	if m.Kind != 'O' || ver == 0 {
		t.Fatalf("Expected write to succeed and return version")
	}

	var wg sync.WaitGroup
	wg.Add(nclients)

	errorCh := make(chan error, nclients)

	for i := 0; i < nclients; i++ {
		go func(i int, ver int, cl *ClientContainer) {
			sem.Wait()
			defer wg.Done()
			for j := 0; j < niters; j++ {
				str := fmt.Sprintf("cl %d %d", i, j)
				for {
					m, err := cl.rCAS("concCas", ver, str, 0)
					if err != nil {
						errorCh <- err
						return
					} else if m.Kind == 'O' {
						break
					} else if m.Kind != 'V' {
						errorCh <- errors.New(fmt.Sprintf("Expected 'V' msg, got %c", m.Kind))
						return
					}
					ver = m.Version // retry with latest version
				}
			}
		}(i, ver, clientContainers[i])
	}

	time.Sleep(5000 * time.Millisecond) // give goroutines a chance
	sem.Done()                         // Start goroutines
	wg.Wait()                          // Wait for them to finish
	select {
	case e := <-errorCh:
		t.Fatalf("Error received while doing cas: %v", e)
	default: // no errors
	}
	m, _ = clientContainers[0].rRead("concCas")
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 3")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
	}

}
func Test_NotLeaderError(t *testing.T) {
	//startIndex(3);

	rafts[1].Process.Kill()
	time.Sleep(10*time.Second)


	cle := mkClient(t, "localhost:8084")
	c := &ClientContainer{cl:cle}

	m, _ := c.rRead("concCas")
	if !(m.Kind == 'C' && strings.HasSuffix(string(m.Contents), " 3")) {
		t.Fatalf("Expected to be able to read after 1000 writes. Got msg.Kind = %d, msg.Contents=%s", m.Kind, m.Contents)
	}

	cleanup()

}




/*
func xTest_NotLeaderError(t *testing.T) {
	cl := mkClient(t, "localhost:8081")
	defer cl.close()
	// Read non-existent file cs733net
	m, _ := cl.read("cs733net")

	if !(m.Kind == 'L'  && strings.HasPrefix(m.Err_redirect_text, "localhost:")) {
		t.Fatal(m)
	}

	cl = mkClient(t, m.Err_redirect_text)
	defer cl.close()
	data := "Cloud fun"
	m, err := cl.write("cs733net", data, 0)
	check(err)
	expect(t, m, &Msg{Kind: 'O'}, "write success", err)

}*/







//----------------------------------------------------------------------
// Utility functions

type Msg struct {
						  // Kind = the first character of the command. For errors, it
						  // is the first letter after "ERR_", ('V' for ERR_VERSION, for
						  // example), except for "ERR_CMD_ERR", for which the kind is 'M'
	Kind              byte
	Filename          string
	Contents          []byte
	Numbytes          int
	Exptime           int // expiry time in seconds
	Version           int
	Err_redirect_text string

}

func (c *ClientContainer) rRead(filename string) (*Msg, error) {

	var m *Msg
	var err error
	var rebuildConn bool
	rebuildConn = false
	rebuildAddress :=""

	for {
		if rebuildConn{
			c.cl = mkNewClient(rebuildAddress)

		}

		cmd := "read " + filename + "\r\n"
		m, err = c.cl.sendRcv(cmd)
		check(err)

		if err != nil { //some error (uninformed leader change) then create new client and loop
			//if c.cl == nil {
			//fmt.Println("conn at error",c.cl.conn)
			//}

			id := int64(rand.Float32() * float32(len(netconfigs) - 1))
			m := fmt.Sprintf("%s:%d", netconfigs[id].Host,8083 )//netconfigs[id].Port
			//fmt.Println("server dead",m)
			rebuildAddress = m
			rebuildConn = true
			//c.cl.conn.Close()


		}else { //check if leader changed or not selected till now

			if !(m.Kind == 'L' || m.Kind == 'X' ) {

				break
			}else {

				rebuildConn = true
				rebuildAddress = m.Err_redirect_text
				//c.cl.conn.Close()
				fmt.Print("error at new client creations")
				//				}
			}

		}

	}
	return m, err
}

func (c *ClientContainer) rWrite( filename string, contents string, exptime int) (*Msg, error) {

	var m *Msg
	var err error
	var rebuildConn bool
	rebuildConn = false
	rebuildAddress :=""

	for {
		if rebuildConn{
			fmt.Println("error 615",c.cl.conn)
			c.cl = mkNewClient(rebuildAddress)
			fmt.Println("error 617",c.cl.conn)


		}

		var cmd string
		if exptime == 0 {
			cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
		} else {
			cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
		}
		cmd += contents + "\r\n"
		m, err = c.cl.sendRcv(cmd)
		check(err)

		if err != nil { //some error (uninformed leader change) then create new client and loop
			//if c.cl == nil {
			//fmt.Println("conn at error",c.cl.conn)
			//}

				fmt.Println("error expected")
			time.Sleep(5 * time.Second)
			id := int64(rand.Float32() * float32(len(netconfigs) - 1))
			m := fmt.Sprintf("%s:%d", netconfigs[id].Host,8083 )//netconfigs[id].Port
			//fmt.Println("server dead",m)
			rebuildAddress = m
			rebuildConn = true
			//c.cl.conn.Close()


		}else { //check if leader changed or not selected till now

			if !(m.Kind == 'L' || m.Kind == 'X' ) {

				break
			}else {

				rebuildConn = true
				rebuildAddress = m.Err_redirect_text
				//c.cl.conn.Close()
				fmt.Print("error at new client creations")
				//				}
			}

		}

	}
	return m, err
}
func (c *ClientContainer) rDelete(filename string) (*Msg, error) {

	var m *Msg
	var err error

	var rebuildConn bool
	rebuildConn = false
	rebuildAddress :=""



	for {

		if rebuildConn{
			c.cl = mkNewClient(rebuildAddress)

		}
		cmd := "delete " + filename + "\r\n"
		m, err = c.cl.sendRcv(cmd)
		check(err)

		if err != nil { //some error (uninformed leader change) then create new client and loop
			//if c.cl == nil {
			//fmt.Println("conn at error",c.cl.conn)
			//}

			id := int64(rand.Float32() * float32(len(netconfigs) - 1))
			m := fmt.Sprintf("%s:%d", netconfigs[id].Host,8083 )//netconfigs[id].Port
			//fmt.Println("server dead",m)
			rebuildAddress = m
			rebuildConn = true
			//c.cl.conn.Close()


		}else { //check if leader changed or not selected till now

			if !(m.Kind == 'L' || m.Kind == 'X' ) {

				break
			}else {

				rebuildConn = true
				rebuildAddress = m.Err_redirect_text
				//c.cl.conn.Close()
				fmt.Print("error at new client creations")
				//				}
			}

		}

	}
	return m, err
}


//marker...
func (c *ClientContainer) rCAS(filename string, version int, contents string, exptime int) (*Msg, error) {

	var m *Msg
	var err error

	var rebuildConn bool
	rebuildConn = false
	rebuildAddress :=""


	for {

		if rebuildConn{
			fmt.Print("something wrong")
			c.cl = mkNewClient(rebuildAddress)

		}

		var cmd string
		if exptime == 0 {
			cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
		} else {
			cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
		}
		//fmt.Println("604")

		cmd += contents + "\r\n"

		m, err = c.cl.sendRcv(cmd)
		//fmt.Println("608")

		check(err)

		if err != nil { //some error (uninformed leader change) then create new client and loop


			id := int64(rand.Float32() * float32(len(netconfigs) - 1))
			m := fmt.Sprintf("%s:%d", netconfigs[id].Host,8083 )//netconfigs[id].Port
			rebuildAddress = m
			rebuildConn = true


		}else { //check if leader changed or not selected till now

			if !(m.Kind == 'L' || m.Kind == 'X' ) {

				break
			}else {
				fmt.Print("something wrong735")

				rebuildConn = true
				rebuildAddress = m.Err_redirect_text
				//c.cl.conn.Close()
				fmt.Print("error at new client creations")
//				}
			}

		}

	}

	return m, err
}
//func (cl *Client) read(filename string) (*Msg, error) {
//	//	return rRead(cl, filename)
//	return nil, nil
//}
//
//func (cl *Client) write(filename string, contents string, exptime int) (*Msg, error) {
//	//return rWrite(cl,filename,contents,exptime)
//	return nil, nil
//}
//
//func (cl *Client) cas(filename string, version int, contents string, exptime int) (*Msg, error) {
//	//return rCAS(cl, filename, version, contents, exptime)
//	return nil, nil
//}
//
//func (cl *Client) delete(filename string) (*Msg, error) {
//	return nil,nil
//}

var errNoConn = errors.New("Connection is closed")

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}
type ClientContainer struct {
	cl *Client
}

func mkClient(t *testing.T, rn_addr string) *Client {
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", rn_addr)
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	return client
}
func mkNewClient(rn_addr string) *Client {
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", rn_addr)
	if err == nil {
		conn, err := net.DialTCP("tcp", nil, raddr)
		if err == nil {
			client = &Client{conn: conn, reader: bufio.NewReader(conn)}
		}
	}
	if err != nil {
		check(err)
	}
	return client
}

func (cl *Client) send(str string) error {
	if cl.conn == nil {
		return errNoConn
	}
	_, err := cl.conn.Write([]byte(str))
	if err != nil {
		err = fmt.Errorf("Write error in SendRaw: %v", err)
		cl.conn.Close()
		cl.conn = nil
	}
	return err
}

func (cl *Client) sendRcv(str string) (msg *Msg, err error) {

	if cl.conn == nil {
		return nil, errNoConn
	}
	err = cl.send(str)
	if err == nil {
		msg, err = cl.rcv()
	}
	//fmt.Println("556")
	return msg, err
}

func (cl *Client) close() {
	if cl != nil && cl.conn != nil {
		cl.conn.Close()
		cl.conn = nil
	}
}

func (cl *Client) rcv() (msg *Msg, err error) {
	// we will assume no errors in server side formatting
	line, err := cl.reader.ReadString('\n')
	if err == nil {
		msg, err = parseFirst(line)
		if err != nil {
			return nil, err
		}
		if msg.Kind == 'C' {
			contents := make([]byte, msg.Numbytes)
			var c byte
			for i := 0; i < msg.Numbytes; i++ {
				if c, err = cl.reader.ReadByte(); err != nil {
					break
				}
				contents[i] = c
			}
			if err == nil {
				msg.Contents = contents
				cl.reader.ReadByte() // \r
				cl.reader.ReadByte() // \n
			}
		}
	}
	if err != nil {
		cl.close()
	}
	//fmt.Println("597")

	return msg, err
}

func parseFirst(line string) (msg *Msg, err error) {
	fields := strings.Fields(line)
	msg = &Msg{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int {
		var i int
		if err == nil {
			if fieldNum >= len(fields) {
				err = errors.New(fmt.Sprintf("Not enough fields. Expected field #%d in %s\n", fieldNum, line))
				return 0
			}
			i, err = strconv.Atoi(fields[fieldNum])
		}
		return i
	}

	if len(fields) == 0 {
		return nil, errors.New("Empty line. The previous command is likely at fault")
	}
	switch fields[0] {
	case "OK": // OK [version]
		msg.Kind = 'O'
		if len(fields) > 1 {
			msg.Version = toInt(1)
		}
	case "CONTENTS": // CONTENTS <version> <numbytes> <exptime> \r\n
		msg.Kind = 'C'
		msg.Version = toInt(1)
		msg.Numbytes = toInt(2)
		msg.Exptime = toInt(3)
	case "ERR_VERSION":
		msg.Kind = 'V'
		msg.Version = toInt(1)
	case "ERR_FILE_NOT_FOUND":
		msg.Kind = 'F'
	case "ERR_CMD_ERR":
		msg.Kind = 'M'
	case "ERR_INTERNAL":
		msg.Kind = 'I'
	case "ERR_REDIRECT":
		msg.Kind = 'L'
		msg.Err_redirect_text = fields[1]
	case "ERR_NLEADER":
		msg.Kind = 'X'
		msg.Err_redirect_text = fields[1]



	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}
func expect(t *testing.T, response *Msg, expected *Msg, errstr string, err error) {
	if err != nil {
		t.Fatal("Unexpected error: " + err.Error())
	}
	ok := true
	if response.Kind != expected.Kind {
		ok = false
		errstr += fmt.Sprintf(" Got kind='%c', expected '%c'", response.Kind, expected.Kind)
	}
	if expected.Version > 0 && expected.Version != response.Version {
		ok = false
		errstr += " Version mismatch"
	}
	if response.Kind == 'C' {
		if expected.Contents != nil &&
		bytes.Compare(response.Contents, expected.Contents) != 0 {
			ok = false
		}
	}
	if !ok {
		t.Fatal("Expected " + errstr)
	}
}
