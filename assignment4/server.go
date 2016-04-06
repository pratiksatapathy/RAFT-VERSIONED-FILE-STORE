package main

import (
	"bufio"
	"fmt"
	"github.com/pratiksatapathy/cs733/assignment3/fs"
	"net"
	"os"
	"strconv"
)
var clientHandles int
var crlf = []byte{'\r', '\n'}

var client_mapper fs.Mapper
func checkk(obj interface{}) {
	if obj != nil {
		fmt.Println(obj)
		os.Exit(1)
	}
}

func reply(conn *net.TCPConn, msg *fs.Msg) bool {
	var err error
	write := func(data []byte) {
		if err != nil {
			return
		}
		_, err = conn.Write(data)
	}
	var resp string
	switch msg.Kind {
	case 'C': // read response
		resp = fmt.Sprintf("CONTENTS %d %d %d", msg.Version, msg.Numbytes, msg.Exptime)
	case 'O':
		resp = "OK "
		if msg.Version > 0 {
			resp += strconv.Itoa(msg.Version)
		}
	case 'F':
		resp = "ERR_FILE_NOT_FOUND"
	case 'V':
		resp = "ERR_VERSION " + strconv.Itoa(msg.Version)
	case 'M':
		resp = "ERR_CMD_ERR"
	case 'I':
		resp = "ERR_INTERNAL"
	default:
		fmt.Printf("Unknown response kind '%c'", msg.Kind)
		return false
	}
	resp += "\r\n"
	write([]byte(resp))
	if msg.Kind == 'C' {
		write(msg.Contents)
		write(crlf)
	}
	return err == nil
}

func serve(conn *net.TCPConn, rn *RaftNode, clientHandle int) {

	//assign a channel to this client

	reader := bufio.NewReader(conn)
	for {
		msg, msgerr, fatalerr := fs.GetMsg(reader)
		if fatalerr != nil || msgerr != nil {
			reply(conn, &fs.Msg{Kind: 'M'})
			conn.Close()
			break
		}

		if msgerr != nil {
			if (!reply(conn, &fs.Msg{Kind: 'M'})) {
				conn.Close()
				break
			}
		}

		channelForClient := make(chan interface{},100)
		client_mapper.ChanDir[clientHandle] = channelForClient

		rn.Append([]byte(fmt.Sprintf("%v", fs.MsgWithId{Handler:clientHandle,Mesg:msg})))

		response := <-client_mapper.ChanDir[clientHandle]

		if !reply(conn, response) {
			conn.Close()

			break
		}

	}
}

func serverMain(port int, rn *RaftNode) {

	clientHandles = 0

	go fs.StartFileStore(rn.CommitChan,client_mapper) //give raft commit channel to fs

	client_mapper = &fs.Mapper{ChanDir: make(map[string]*net.TCPConn, 1000)}
	tcpaddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", port))
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)

	check(err)

	for {
		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)
		clientHandles ++ ;
		go serve(tcp_conn, rn, clientHandles)
	}
}

func main() {
	//serverMain()
}
