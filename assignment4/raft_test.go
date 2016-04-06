package main

import (
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"testing"
	"os"
	"time"
	"sync"
	"fmt"
	"net"
	"errors"
	"bufio"
	"strings"
	"strconv"
)

var Rafts []RaftNode
var mck *mock.MockCluster
//func init() {
//
//	startRaft()
//}

//starting rafter

func startRaft() {
	cleanSlate()//cleanup before begining

	//fmt.Print("TEST TAKES APPROX 180 SECS/ PLEASE HOLD ON")
	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
		{Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
	}}
	var err error
	mck, err = mock.NewCluster(clconfig)
	Rafts = rafter(cluster) // array of []raft.Node

	check(err)


}

//cleaning logs before one more test instance starts
func cleanSlate() {

	for i := 0; i < len(Rafts); i++ {
		Rafts[i].ShutDown()
		time.Sleep(1 * time.Second)

	}

	os.RemoveAll("dir1"); os.RemoveAll("dir2"); os.RemoveAll("dir3"); os.RemoveAll("dir4"); os.RemoveAll("dir5")
	//err = os.Remove("dir1.txt"); os.Remove("dir2.txt"); os.Remove("dir3.txt"); os.Remove("dir4.txt"); os.Remove("dir5.txt")

	//check(err)

}
func TestOne(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)

	startRaft()

	//mck.Partition()
	time.Sleep(1 * time.Second)

	go serverMain(8080, &Rafts[0])
	go serverMain(8081, &Rafts[1])
	go serverMain(8082, &Rafts[2])
	go serverMain(8083, &Rafts[3])
	go serverMain(8084, &Rafts[4])

	time.Sleep(4 * time.Second)
	fmt.Println("fileservers up")
	time.Sleep(4 * time.Second)

	cl := mkClient(t)
	defer cl.close()
	data := "Cloud fun"
	m, err := cl.write("cs733net", data, 0)
	fmt.Println("content:",string(m.Contents))
	check(err)

	wg.Wait()

}


//----------------------------------------------------------------------
// Utility functions

type Msg struct {
				 // Kind = the first character of the command. For errors, it
				 // is the first letter after "ERR_", ('V' for ERR_VERSION, for
				 // example), except for "ERR_CMD_ERR", for which the kind is 'M'
	Kind     byte
	Filename string
	Contents []byte
	Numbytes int
	Exptime  int // expiry time in seconds
	Version  int
}

func (cl *Client) read(filename string) (*Msg, error) {
	cmd := "read " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) write(filename string, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("write %s %d\r\n", filename, len(contents))
	} else {
		cmd = fmt.Sprintf("write %s %d %d\r\n", filename, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) cas(filename string, version int, contents string, exptime int) (*Msg, error) {
	var cmd string
	if exptime == 0 {
		cmd = fmt.Sprintf("cas %s %d %d\r\n", filename, version, len(contents))
	} else {
		cmd = fmt.Sprintf("cas %s %d %d %d\r\n", filename, version, len(contents), exptime)
	}
	cmd += contents + "\r\n"
	return cl.sendRcv(cmd)
}

func (cl *Client) delete(filename string) (*Msg, error) {
	cmd := "delete " + filename + "\r\n"
	return cl.sendRcv(cmd)
}

var errNoConn = errors.New("Connection is closed")

type Client struct {
	conn   *net.TCPConn
	reader *bufio.Reader // a bufio Reader wrapper over conn
}

func mkClient(t *testing.T) *Client {
	var client *Client
	raddr, err := net.ResolveTCPAddr("tcp", "localhost:8080")
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
	return msg, err
}

func parseFirst(line string) (msg *Msg, err error) {
	fields := strings.Fields(line)
	msg = &Msg{}

	// Utility function fieldNum to int
	toInt := func(fieldNum int) int {
		var i int
		if err == nil {
			if fieldNum >=  len(fields) {
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
	default:
		err = errors.New("Unknown response " + fields[0])
	}
	if err != nil {
		return nil, err
	} else {
		return msg, nil
	}
}
