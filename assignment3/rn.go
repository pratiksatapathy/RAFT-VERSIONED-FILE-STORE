package main
import (
	"github.com/cs733-iitb/log"
	"io/ioutil"
	"strings"
	"time"
	"reflect"
	"github.com/cs733-iitb/cluster"
	"fmt"
	"encoding/gob"
	"strconv"
//"os"
	"sync"
	"github.com/cs733-iitb/cluster/mock"
)

var MutX_findLeader *sync.Mutex
var MutX_getcandidatearray *sync.Mutex
var MutX_processEvent *sync.Mutex



type RaftNode struct { // implements Node interface

	EventCh       chan interface{}
	TimeoutCh     chan interface{}
	CommitChan    chan COMMIT_TO_CLIENT
	LogHandler    *log.Log
	myconfig      Config
					   //MsgBoxHandler cluster.Server
	MsgBoxHandler *mock.MockServer
	mockServers   *mock.MockCluster


					   //Term       int64
					   //VoteFor    int64
	Sm            SERVER_DATA
	Cluster       []NetConfig
	LastAlarm     time.Time
	StopSignal    bool
	LogDir        string
}
//type CommitInfo struct {
//
//	Data  []byte
//	Index int64 // or int .. whatever you have in your code
//	Err   error
//}

type Config struct {

	Cluster          []NetConfig // Information about all servers, including this.
	Id               int         // this node's id. One of the cluster's entries should match.
	LogDir           string      // Log file directory for this node
	ElectionTimeout  int64
	HeartbeatTimeout int64

}

type NetConfig struct {

	Id   int
	Host string
	Port int
}

//starting point

//func debug_output(i interface{}) {
//
//
//	//	fmt.Print(i)
//	//	fmt.Println()
//}
//func (rn *RaftNode) debug_output3(s string, i interface{}) {
//	//logit(rn.LogDir, fmt.Sprintln(i))
//}
//func (rn *RaftNode) debug_output2(s string, i interface{}) {
//
//
//
//	//	//logit(rn.LogDir,fmt.Sprint("**********NOde Id:"))
//	//	//logit(rn.LogDir,fmt.Sprint(rn.Sm.candidateId))
//	//	logit(rn.LogDir, fmt.Sprint(s))
//	//	logit(rn.LogDir, fmt.Sprintln(i))
//	//	//logit(rn.LogDir,fmt.Sprint())
//	//	//logit(rn.LogDir,fmt.Sprint("********end********"))
//
//}
//
///*
//func (rn *RaftNode) debug_output2(s string,i interface{} ){
//
//	//return
//
//	fmt.Print("**********NOde Id:")
//	fmt.Println(rn.Sm.candidateId)
//	fmt.Print(s)
//	fmt.Print(i)
//	fmt.Println()
//	fmt.Println("********end********")
//
//}
//*/
//func (rn *RaftNode) debug_output_act(s string, i []interface{}) {
//
//
//
////	//logit(rn.LogDir,fmt.Sprintln("******"))
////	//logit(rn.LogDir,fmt.Sprintln(rn.Sm.candidateId))
////	logit(rn.LogDir, fmt.Sprintln(s))
////	for _, element := range i {
////
////		logit(rn.LogDir, fmt.Sprint(reflect.TypeOf(element)))
////		logit(rn.LogDir, fmt.Sprint(element))
////		//logit(rn.LogDir,fmt.Sprint(","))
////		break
////	}
////
//	//logit(rn.LogDir,fmt.Sprintln())
//	//logit(rn.LogDir,fmt.Sprintln("*****"))
//
//
//}

//func debug_output2(s string, i interface{}) {
//
//
////
////	fmt.Print(s)
////	fmt.Print(i)
////	fmt.Println()
////	fmt.Println("***")
//
//}
//func logit(filename string, text string) {
//	//return
//	f, err := os.OpenFile(filename + ".txt", os.O_APPEND | os.O_WRONLY, 0600)
//	if err != nil {
//		panic(err)
//	}
//
//	defer f.Close()
//
//	if _, err = f.WriteString(text); err != nil {
//		panic(err)
//	}
//
//}
//func main23() {
//	var wg sync.WaitGroup
//
//	wg.Add(1)
//	//fmt.Print("kyazoonga")
//	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
//		{Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
//	}}
//	cluster, err := mock.NewCluster(clconfig)
//
//	cluster.Partition([]int{1, 2, 3}, []int{4, 5}) // Cluster partitions into two.
//
//
//	rafts := rafter2(cluster) // array of []raft.Node
//
//	time.Sleep(10 * time.Second)
//	(rafts[findLeader(rafts) - 1]).Append([]byte("foo"))
//	time.Sleep(20 * time.Second)
//	cluster.Heal()
//	time.Sleep(10 * time.Second)
//	fmt.Println("healeddddddddddddddddddddddddddddd\n")
//
//	(rafts[findLeader(rafts) - 1]).Append([]byte("bar"))
//
//	check(err)
//	wg.Wait()
//
//	//rafter()
//}
func rafter(mck *mock.MockCluster) ([]RaftNode) {

	MutX_findLeader = &sync.Mutex{}
	MutX_getcandidatearray = &sync.Mutex{}
	MutX_processEvent = &sync.Mutex{}
	//debug_output(mck)


	registerAllStructures();

	var netconfigs []NetConfig
	//netconfigs = append(netconfigs,NetConfig{Id:1,Host:"localhost",Port:8001})

	netconfigs = []NetConfig{NetConfig{Id:1, Host:"localhost", Port:8001},
		NetConfig{Id:2, Host:"localhost", Port:8002},
		NetConfig{Id:3, Host:"localhost", Port:8003},
		NetConfig{Id:4, Host:"localhost", Port:8004},
		NetConfig{Id:5, Host:"localhost", Port:8005}}
	//debug_output(netconfigs)

	var configs []Config

	configs = []Config{Config{Cluster:netconfigs, Id:1, LogDir:"dir1", ElectionTimeout:2500, HeartbeatTimeout:500},
		Config{Cluster:netconfigs, Id:2, LogDir:"dir2", ElectionTimeout:2400, HeartbeatTimeout:500},
		Config{Cluster:netconfigs, Id:3, LogDir:"dir3", ElectionTimeout:2300, HeartbeatTimeout:500},
		Config{Cluster:netconfigs, Id:4, LogDir:"dir4", ElectionTimeout:2200, HeartbeatTimeout:500},
		Config{Cluster:netconfigs, Id:5, LogDir:"dir5", ElectionTimeout:2100, HeartbeatTimeout:500}}

	//var rafts []RaftNode
	rafts := makeRaftNodes(configs, mck);


	go func() {
		var wg sync.WaitGroup

		wg.Add(1)
		rafts[0].startOperation()
		rafts[1].startOperation()
		rafts[2].startOperation()
		rafts[3].startOperation()
		rafts[4].startOperation()

		wg.Wait()

	}()

	return rafts
}


//func rafter2(mck *mock.MockCluster) ([]RaftNode) {
//
//	fmt.Print("===========\n")
//	fmt.Println(reflect.TypeOf(mck))
//	debug_output(mck)
//
//
//	registerAllStructures();
//
//	var netconfigs []NetConfig
//	//netconfigs = append(netconfigs,NetConfig{Id:1,Host:"localhost",Port:8001})
//
//	netconfigs = []NetConfig{NetConfig{Id:1, Host:"localhost", Port:8001},
//		NetConfig{Id:2, Host:"localhost", Port:8002},
//		NetConfig{Id:3, Host:"localhost", Port:8003},
//		NetConfig{Id:4, Host:"localhost", Port:8004},
//		NetConfig{Id:5, Host:"localhost", Port:8005}}
//	debug_output(netconfigs)
//
//
//	var configs []Config
//
//	configs = []Config{Config{Cluster:netconfigs, Id:1, LogDir:"dir1", ElectionTimeout:6, HeartbeatTimeout:1},
//		Config{Cluster:netconfigs, Id:2, LogDir:"dir2", ElectionTimeout:6, HeartbeatTimeout:1},
//		Config{Cluster:netconfigs, Id:3, LogDir:"dir3", ElectionTimeout:6, HeartbeatTimeout:1},
//		Config{Cluster:netconfigs, Id:4, LogDir:"dir4", ElectionTimeout:7, HeartbeatTimeout:1},
//		Config{Cluster:netconfigs, Id:5, LogDir:"dir5", ElectionTimeout:7, HeartbeatTimeout:1}}
//
//
//	//var rafts []RaftNode
//	rafts := makeRaftNodes(configs, mck);
//
//	//fmt.Println(rafts)
//
//	//	var wg sync.WaitGroup
//	//
//	//	wg.Add(1)
//	rafts[0].startOperation()
//	rafts[1].startOperation()
//	rafts[2].startOperation()
//	rafts[3].startOperation()
//	rafts[4].startOperation()
//
//	time.Sleep(time.Second * 10)
//
//	//	rafts[findLeader(rafts) - 1].Append([]byte("cmd1"))
//	//	time.Sleep(time.Millisecond * 1000)
//	//	time.Sleep(time.Second * 50)
//	//	(rafts[findLeader(rafts) - 1]).Append([]byte("foo"))
//	//	rafts[findLeader(rafts) - 1].Append([]byte("cmd2"))
//	//	time.Sleep(time.Millisecond * 1000)
//	//	rafts[findLeader(rafts) - 1].Append([]byte("cmd3"))
//	//	time.Sleep(time.Millisecond * 1000)
//	//	rafts[findLeader(rafts) - 1].Append([]byte("cmd4"))
//
//
//	//time.Sleep(time.Second * 10)
//
//
//	//	time.Sleep(time.Second * 10)
//	//	fmt.Println("=====================================================================")
//	//	fmt.Println(rafts[0].Sm.leaderId)
//	//	rafts[findLeader(rafts)-1].ShutDown()
//	//
//	//	time.Sleep(time.Second * 10)
//	//	fmt.Println("=====================================================================")
//	//	fmt.Println(rafts[0].Sm.leaderId)
//	//	rafts[findLeader(rafts)-1].ShutDown()
//	//
//	//
//	//	time.Sleep(time.Second * 10)
//	//	fmt.Println("=====================================================================")
//	//	fmt.Println(rafts[0].Sm.leaderId)
//	//	rafts[findLeader(rafts)-1].ShutDown()
//	//
//	//
//	//	time.Sleep(time.Second * 20)
//	//
//	//
//	//	for i:=0;i<len(rafts);i++ {
//	//		if rafts[i].StopSignal == true{
//	//
//	//			time.Sleep(time.Second * 20)
//	//			rafts[i].StopSignal = false
//	//			rafts[i].startOperation()
//	//
//	//		}
//	//
//	//	}
//	//
//	//	for _, element := range rafts {
//	//
//	//		if element.StopSignal == true{
//	//
//	//			time.Sleep(time.Second * 20)
//	//			element.StopSignal = false
//	//			element.startOperation()
//	//
//	//		}
//	//
//	//	}
//	//
//	//
//	//
//
//	//wg.Wait()
//	return rafts
//}

//function to determine the leader, we are also checking that if in a partition environment there are two
// leader then only the latest term leader is chosen as the leader
func findLeader(rafts []RaftNode) (index int) {

	MutX_findLeader.Lock()
	//initializing to undefined
	index = UNDEF
	leaderTerm := UNDEF
	for {
		for _, element := range rafts {

			if element.StopSignal == false {

				if element.Sm.state == LEADER {

					if index == UNDEF && leaderTerm == UNDEF {
						index = int(element.Sm.candidateId)
						leaderTerm = int(element.Sm.term)
					}else if int(element.Sm.term) > leaderTerm {
						index = int(element.Sm.candidateId)
						leaderTerm = int(element.Sm.term)
					}

				}

			}

		}
		if index != UNDEF{
				//fmt.Println("leader mila ")
			break
		}else{
			//fmt.Print(".. ")
		}
	}
	MutX_findLeader.Unlock()
	return int(index)

}
func makeRaftNodes(configs []Config, mck *mock.MockCluster) (rafts []RaftNode) {

	//var node RaftNode

	rafts = []RaftNode{startNewRaftNode(configs[0], mck),
		startNewRaftNode(configs[1], mck),
		startNewRaftNode(configs[2], mck),
		startNewRaftNode(configs[3], mck),
		startNewRaftNode(configs[4], mck)}

	check(nil)

	return rafts
}

func startNewRaftNode(config Config, mck *mock.MockCluster) (node RaftNode) {


	node.myconfig = config
	//debug_output(config)
	//create state machine in follower mode
	node.initializeStateMachine(config)

	//init the log
	node.initializeLog(config)

	//init messaging framework
	node.initializeMessageBox(config, mck)

	//read log from stored log //to be implemented

	node.readLogsToStateMachine(config)

	//read term and vote
	node.initializeVoteAndTerm(config)

	//assign the cluster
	//node.Cluster = config.Cluster

	//node.debug_output2("setup complete", "")


	//for{}
	//	wg.Done()
	return node
}

func (node *RaftNode) startOperation() {

	//start listening to other nodes
	go node.startListening()

	//start state machine processor
	go node.processEvents()

	//start timer
	go node.startTimer()
}

func (rn *RaftNode) initializeLog(config Config) {

	var err error

	handler, err := log.Open(config.LogDir + "/raftlog")
	rn.LogHandler = handler
		fmt.Println("chk442\n")
	check(err)
		fmt.Println("chk444\n")

	//rn.debug_output2("1:log object completed", rn.LogHandler)
	//rn.debug_output2("1:lastindex", rn.LogHandler.GetLastIndex())
	if rn.LogHandler.GetLastIndex() == -1 { //log is absolutely empty

		rn.LogHandler.Append(SERVER_LOG_DATASTR{Index:0, Term:0, Data:[]byte("")})
	}

	//rn.debug_output2("2:log object completed", rn.LogHandler)
	//rn.debug_output2("2:lastindex", rn.LogHandler.GetLastIndex())
}
func (rn *RaftNode) readLogsToStateMachine(config Config) {
	//return
	lastIndex := rn.LogHandler.GetLastIndex()

	counter := int64(0)
	//var err error
	for counter = 0; counter <= lastIndex; counter++ {

		lg, err := rn.LogHandler.Get(counter)
		check(err)
		//rn.debug_output2("what in storage", lg)
		rn.Sm.LOG[counter] = lg.(SERVER_LOG_DATASTR)

	}
	//debug_output("log reading:")
	//rn.debug_output2("recovered log", rn.Sm.LOG)


}
func check(err error) {

	if err != nil {
		fmt.Println(err)
	}

}
func registerAllStructures() {

	gob.Register(VOTE_REQUEST{})
	gob.Register(VOTE_RESPONSE{})
	gob.Register(APPEND_ENTRIES_REQUEST{})
	gob.Register(APPEND_ENTRIES_RESPONSE{})
	gob.Register(SERVER_LOG_DATASTR{})
}
func (rn *RaftNode) initializeMessageBox(config Config, mck *mock.MockCluster) {


	rn.Cluster = config.Cluster
	var err error

	//rn.debug_output2("raftermsg:", len(mck.Servers))//

	rn.MsgBoxHandler = mck.Servers[int(rn.Sm.candidateId)]


	check(err)

}

func (rn *RaftNode) initializeVoteAndTerm(config Config) {



	fileContent, e := ioutil.ReadFile(config.LogDir + "/termNvote.txt")
	//rn.logHandler = log.Open(config.logDir + "/raftlog")


	if e == nil {


		arr := strings.Split(string(fileContent), ":")

		term, err := strconv.Atoi(arr[0])
		rn.Sm.term = int64(term)
		check(err)

		voted, err := strconv.Atoi(arr[0])
		rn.Sm.votedFor = int64(voted)
		check(err)

	}else {//no state stored previously

		ioutil.WriteFile(config.LogDir + "/termNvote.txt", []byte("0:0"), 0644)
		rn.Sm.term = 0
		rn.Sm.votedFor = 0
	}
	//debug_output("votedir done")
	//debug_output(rn.Sm.term)
	//debug_output(rn.Sm.votedFor)

}
func (rn *RaftNode) writeVoteAndTerm(state_SM STATE_STORE) {

	//rn.debug_output2("statestorestofile", rn.LogDir)
	e := ioutil.WriteFile(rn.LogDir + "/termNvote.txt", []byte(fmt.Sprintf("%d:%d", state_SM.term, state_SM.voteFor)), 0644)

	check(e)
}
func (rn *RaftNode) initializeStateMachine(config Config) {

	rn.LogDir = config.LogDir

	rn.Sm.candidateId = int64(config.Id)
	//rn.debug_output2("*********Node coming up********", "")
	rn.Sm.state = FOLLOWER
	rn.LastAlarm = time.Now()
	//svr.leaderId = 3
	//config.cluster
	rn.Sm.candidates = getCandidateIdArray(config) //return candidate array from config netconfig[] array
	//	debug_output(rn.Sm.candidates)
	//	rn.Sm.candidates = []int64{1, 2, 3, 4, 5}
	//svr.commitIndex =
	rn.Sm.election_time_out = config.ElectionTimeout; //secs
	rn.Sm.heartbeat_time_out = config.HeartbeatTimeout; //secs

	rn.Sm.LOG = make(map[int64]SERVER_LOG_DATASTR)
	//svr.addThisEntry(SERVER_LOG_DATASTR{index:0,term:0,data:[]byte("")}) // dummy log at zeroth index on all servers that are starting up


	//debug_output("state machine completed")

	//channel allocation
	rn.EventCh = make(chan interface{})
	rn.TimeoutCh = make(chan interface{})
	rn.CommitChan = make(chan COMMIT_TO_CLIENT, 100)


}

func (rn *RaftNode) startTimer() {


	//rn.debug_output2("timer started..", "")
	rn.LastAlarm = time.Now()
	for {

		//to have a stop mark at shutdown
		//to have a lock for concurrency control

		//		if time.Now().After(rn.LastAlarm.Add(rn.Sm.heartbeat_time_out * time.Second)) {
		//			rn.TimeoutCh <- TIMEOUT{}
		//		}


		if rn.Sm.state == LEADER {

			if time.Now().After(rn.LastAlarm.Add(time.Duration(rn.Sm.heartbeat_time_out * 1) * time.Millisecond)) {
				rn.LastAlarm = time.Now()
				rn.TimeoutCh <- TIMEOUT{}
				//rn.debug_output3("L: ", time.Now())
			}
		}else {
			//debug_output(".")

			if time.Now().After(rn.LastAlarm.Add(time.Duration(rn.Sm.election_time_out * 1) * time.Millisecond)) {
				rn.LastAlarm = time.Now()
				//debug_output("timeout fired")
				rn.TimeoutCh <- TIMEOUT{}
				//rn.debug_output2("timeout fired", "")
				//rn.debug_output3("F/C :", time.Now())
			}
		}


		if rn.StopSignal {
			//rn.debug_output2("stopping..timer", "")
			return
		}

	}
}

func (rn *RaftNode) startListening() {


	//rn.debug_output2("listening loop started..", "")


	for {
		//rn.debug_output2("node listening","")
		//rn.debug_output2("node listening",rn.MsgBoxHandler.Peers())
		//rn.debug_output2("node listening",rn.MsgBoxHandler.Pid())
		//rn.debug_output2("node listening",rn.MsgBoxHandler.IsClosed())
		//env := <-rn.MsgBoxHandler.Inbox()

		env := <-rn.MsgBoxHandler.Inbox()
		//		if rn.Sm.state == FOLLOWER {
		//
		//			if reflect.TypeOf(env) == reflect.TypeOf(APPEND_ENTRIES_REQUEST {}){
		//
		//				r:=env.Msg.(APPEND_ENTRIES_REQUEST)
		//				if r.IsHeartbeat ==false {
		//
		//					rn.debug_output2("XXXXXXXXXXXXXXXXXXXXXXXXXXXXX",APPEND_ENTRIES_REQUEST{})
		//
		//				}
		//
		//			}
		//		}

		if rn.StopSignal {
			//rn.debug_output2("stopping..msgbox", "")
			return
		}

		var container interface{}
		container = env.Msg

		//	rn.debug_output2("->", reflect.TypeOf(container))
		//		if reflect.TypeOf(container) == reflect.TypeOf(APPEND_ENTRIES_REQUEST{}) {
		//			rn.debug_output2("|isHB:", container.(APPEND_ENTRIES_REQUEST).IsHeartbeat)
		//		}
		//		if reflect.TypeOf(container) == reflect.TypeOf(APPEND_ENTRIES_RESPONSE{}) {
		//			rn.debug_output2("appResponse:", container.(APPEND_ENTRIES_RESPONSE))
		//		}


		rn.EventCh <- container
		//rn.debug_output2("+->", container)
		//	rn.debug_output2("|||", rn.Sm.LOG)
		//	rn.debug_output2("Loghandler: ", rn.LogHandler)
		if rn.LogHandler != nil {
			hardlog, err := rn.LogHandler.Get(rn.LogHandler.GetLastIndex())
			check(err)
			if hardlog != nil {

				//rn.debug_output2("|HARDLOG|", string((hardlog).(SERVER_LOG_DATASTR).Data))

			}
		}


	}
}


func (rn *RaftNode) closeThings() {
	rn.LogHandler.Close()
	rn.MsgBoxHandler.Close()

}




func (rn *RaftNode) processEvents() {


	//rn.debug_output2("process event loop started..", "")


	var ev interface{}
	for {
		//debug_output(".")

		select {

		case ev = <-rn.EventCh:
		case ev = <-rn.TimeoutCh:

		}
		//if rn.Sm.state == LEADER {
		//rn.debug_output2("event", ev)
		//}
		//debug_output("timeout received")
		//rn.debug_output2(" ele to",rn.Sm.election_time_out)

		if rn.StopSignal {
			//rn.debug_output2("stopping..procesevent", "")


			return
		}

		//rn.debug_output2("event", ev)

		MutX_processEvent.Lock()
		actions := rn.Sm.processEvent(ev)
		MutX_processEvent.Unlock()

		//if rn.Sm.state == LEADER {
		//rn.debug_output_act("actio", actions)
		//}

		//		rn.debug_output2("term",rn.Sm.term)
		//		rn.debug_output2("i am a : ",rn.Sm.state)
		//		rn.debug_output_act("show actions:",actions)
		rn.doActions(actions)

		//		if rn.Sm.state == LEADER {
		//			rn.debug_output2("=================", "ok done all")
		//		}

	}
}
func getCandidateIdArray(config Config) ([]int64) {

	MutX_getcandidatearray.Lock()

	cids := make([]int64, 0)

	for i := 0; i < len(config.Cluster); i++ {
		cids = append(cids, int64(config.Cluster[i].Id))
	}

	MutX_getcandidatearray.Unlock()
	return cids
}
func (rn *RaftNode) doActions(actions []interface{}) {

	for i := 0; i < len(actions); i++ {
		//to precess each record by type

		if rn.Sm.state == LEADER {

			//rn.debug_output2("doingAction", reflect.TypeOf(actions[i]))
		}


		if reflect.TypeOf(actions[i]) == reflect.TypeOf(VOTE_REQUEST{}) {

			rn.forwardVoteRequest(actions[i].(VOTE_REQUEST))
		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(VOTE_RESPONSE{}) {

			rn.forwardVoteResponse(actions[i].(VOTE_RESPONSE))

		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(APPEND_ENTRIES_REQUEST{}) {

			rn.forwardAppendEntriesRequest(actions[i].(APPEND_ENTRIES_REQUEST))

		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(APPEND_ENTRIES_RESPONSE{}) {

			rn.forwardAppendEntriesResponse(actions[i].(APPEND_ENTRIES_RESPONSE))

		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(COMMIT_TO_CLIENT{}) {

			//rn.debug_output2("COmmit receivedZZZZZZZZZZZZZZZZZZZZZZZZZZ", rn.Sm.candidateId)
			//fmt.Println("ZZZZZZZZZZZZZZZZZZZZZZZZZZ")
			rn.forwardToClient(actions[i].(COMMIT_TO_CLIENT))

		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(SERVER_LOG_DATASTR{}) {

			//rn.debug_output2("it came here", actions[i])
			rn.saveToLog(actions[i].(SERVER_LOG_DATASTR))

		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(RESET_ALARM_ELECTION_TIMEOUT{}) {

			rn.setElectionTimeout(actions[i].(RESET_ALARM_ELECTION_TIMEOUT))

		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(RESET_ALARM_HEARTBEAT_TIMEOUT{}) {

			rn.setHeartbeatTimeout(actions[i].(RESET_ALARM_HEARTBEAT_TIMEOUT))

		}else if reflect.TypeOf(actions[i]) == reflect.TypeOf(STATE_STORE{}) {

			rn.storeTermAndVote(actions[i].(STATE_STORE))

		}
	}

}

func (rn *RaftNode) forwardVoteRequest(vrq_SM VOTE_REQUEST) {

	//rn.debug_output2("vote request to ",vrq_SM.To_CandidateId)
	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: int(vrq_SM.To_CandidateId), Msg: vrq_SM}


}

func (rn *RaftNode) forwardVoteResponse(vr_SM VOTE_RESPONSE) {


	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: int(vr_SM.To_CandidateId), Msg:vr_SM}

}
func (rn *RaftNode) forwardAppendEntriesRequest(arq_SM APPEND_ENTRIES_REQUEST) {

	//rn.debug_output2("theapReq", arq_SM)
	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: int(arq_SM.To_CandidateId), Msg: arq_SM}
	//if arq_SM.IsHeartbeat == false{
	//rn.debug_output2("Done", arq_SM)
	//}

}
func (rn *RaftNode) forwardAppendEntriesResponse(ars_SM APPEND_ENTRIES_RESPONSE) {

	//rn.debug_output2("append response to ", ars_SM)
	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: int(ars_SM.To_CandidateId), Msg:ars_SM}


}
func (rn *RaftNode) forwardToClient(cmt_to_c COMMIT_TO_CLIENT) {

	//debug_output("commit sent from:")

	//debug_output(rn.Sm.candidateId)

	rn.CommitChan <- cmt_to_c

}
func (rn *RaftNode) saveToLog(lg_SM SERVER_LOG_DATASTR) {

	//rn.LogHandler.Lock()
	rn.LogHandler.TruncateToEnd(lg_SM.Index) //not mandatory
	//rn.debug_output2("saved_state_disk:", lg_SM)
	rn.LogHandler.Append(lg_SM)
	//rn.LogHandler.Unlock()

}
func (rn *RaftNode) setElectionTimeout(vr_SM RESET_ALARM_ELECTION_TIMEOUT) {

	rn.LastAlarm = time.Now()


	if rn.Sm.state == CANDIDATE {

		//rn.debug_output2("started election ", "")

	}else {

		//msg:= fmt.Sprint("L: %d, T: %d",int(rn.Sm.leaderId),int(rn.Sm.term))

		//rn.debug_output2("reset election timeout ", "")
		//rn.debug_output2("L:", (rn.Sm.leaderId))
		//rn.debug_output2("T:", (rn.Sm.term))
		//rn.debug_output2("C:", (rn.Sm.commitIndex))

	}

}
func (rn *RaftNode) setHeartbeatTimeout(vr_SM RESET_ALARM_HEARTBEAT_TIMEOUT) {

	rn.LastAlarm = time.Now()
	//rn.debug_output2("send hrtbt  ", rn.Sm.leaderId)
	//rn.debug_output2("T:", (rn.Sm.term))

}
func (rn *RaftNode) storeTermAndVote(st_SM STATE_STORE) {


	rn.writeVoteAndTerm(st_SM)

}

//********************************************INTERFACE_METHODS******************************************
func (rn *RaftNode) Append(data []byte) {

	rn.EventCh <- data
}
func (rn *RaftNode) CommitChannel() (chan COMMIT_TO_CLIENT) {

	return rn.CommitChan
}
//to implement locking here
func (rn *RaftNode) CommittedIndex() (ci int64) {

	return rn.Sm.commitIndex
}
func (rn *RaftNode) GetIndex(ci int64) (bt []byte, e error) {

	lg, err := rn.LogHandler.Get(ci)
	//check(err)
	e = err
	bt = (lg.(SERVER_LOG_DATASTR)).Data

	return bt, e
}
func (rn *RaftNode) Id() (int64) {

	return rn.Sm.candidateId
}
func (rn *RaftNode) LeaderId() (int64) {

	return rn.Sm.leaderId
}
func (rn *RaftNode) ShutDown() {

	//setting a flag to stop timer go routine
	rn.StopSignal = true
	//rn.closeThings()
}

//********************************************************************************************************
