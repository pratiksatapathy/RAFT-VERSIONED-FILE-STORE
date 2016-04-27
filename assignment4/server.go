package main

import (
	"bufio"
	"fmt"
	"github.com/pratiksatapathy/cs733/assignment4/fs"
	"github.com/pratiksatapathy/cs733/assignment4/raftnode"
	"net"
	"os"
	"strconv"
	"github.com/cs733-iitb/cluster/mock"
	"encoding/json"
	"sync"
	"io/ioutil"
	"github.com/cs733-iitb/cluster"
)
var crlf = []byte{'\r', '\n'}
var mck *mock.MockCluster//var client_mapper *fs.Mapper
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
	case 'L':
		resp = "ERR_REDIRECT " + (msg.Err_redirect_text)
	case 'X':
		resp = "ERR_NLEADER " + (msg.Err_redirect_text)
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

func serve(conn *net.TCPConn, rn *raftnode.RaftNode, clientHandle int, client_mapper *fs.Mapper) {

	//assign a channel to this client

	if rn.IAmNotLeader() {

		node_addr,stat :=rn.GetLeaderURL()

		if  stat {

			fmt.Println("S:notleader")
			reply(conn, &fs.Msg{Kind: 'L',Err_redirect_text:node_addr})

		}else{

			reply(conn, &fs.Msg{Kind: 'X',Err_redirect_text:node_addr})
		}

		conn.Close()
		return


	}else {

		reader := bufio.NewReader(conn)
		for {
			//fmt.Println("got data")
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

			channelForClient := make(chan interface{}, 10000)
			//client_mapper.MutX_ClientMap.
			client_mapper.MutX_ClientMap.Lock()

			client_mapper.ChanDir[clientHandle] = channelForClient
			client_mapper.MutX_ClientMap.Unlock()

			//rn.Append([]byte(fmt.Sprintf("%v", fs.MsgWithId{Handler:clientHandle,Mesg:msg})))

			msgWithId := fs.MsgWithId{Handler:clientHandle, Mesg:*msg}

			bytData, err := json.Marshal(msgWithId)

			if err != nil {
				panic(err)
			}
			//fmt.Println("pdata")


			rn.Append(bytData)
			client_mapper.MutX_ClientMap.RLock()
			channelForThisHandle :=  client_mapper.ChanDir[clientHandle]
			client_mapper.MutX_ClientMap.RUnlock()
			channelOutputForThisHandle := <-channelForThisHandle

			response := channelOutputForThisHandle.(*fs.Msg)
			//panic("")
			//fmt.Println("will respond")
			if !reply(conn, response) {
				conn.Close()

				break
			}

		}
	}
}

func serverMain(id int, config raftnode.Config, mck cluster.Server) {
	//

	clientHandles := 0
	rn := raftnode.StartNewRaftNode(config, mck)
	rn.StartOperation()

	client_mapper := &fs.Mapper{ChanDir: make(map[int]chan interface{}, 1000)}
	client_mapper.MutX_ClientMap = &sync.RWMutex{}
	go fs.StartFileStore(&rn, client_mapper) //give raft commit channel to fs

	tcpaddr, err := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", config.Cluster[id-1].Port))
	//fmt.Println("port: ",config.Cluster[id-1].Port)
	check(err)
	tcp_acceptor, err := net.ListenTCP("tcp", tcpaddr)

	check(err)

	for {
		tcp_conn, err := tcp_acceptor.AcceptTCP()
		check(err)
		clientHandles ++;//fmt.Println("new client..at server")
		go serve(tcp_conn, &rn, clientHandles, client_mapper)
	}
}

func main() {


	var configs []raftnode.Config
	tmp_cnfg,_ := ioutil.ReadFile("configs.json")
	err :=json.Unmarshal(tmp_cnfg,&configs)
	check(err)

//	var cluster_config cluster.Config
//	tmp_clusterConfig,_ := ioutil.ReadFile("clusterconfig.json")
//	err = json.Unmarshal(tmp_clusterConfig,&cluster_config)
//	check(err)

	id,err1 := strconv.Atoi(os.Args[1])
	check(err1)

	//

	server, err := cluster.New(id, "cluster_test_config.json")
	fmt.Println("port: ",configs[id-1].Cluster[id-1].Port,id)
	//ioutil.WriteFile("welcome.txt",[]byte(s),0644)
	serverMain(id,(configs[id-1]),server)
	//serverMain(1,configs[0],mck)


}
func check(err error) {

	if err != nil {
		fmt.Println("ERROR:",err)
	}

}
