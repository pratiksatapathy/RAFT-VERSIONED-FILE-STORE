package main
import (
	"github.com/cs733-iitb/log"
	"io/ioutil"
	"strings"
	"time"
	"reflect"
	"github.com/cs733-iitb/cluster"
	"fmt"
	"encoding/json"
)

type RaftNode struct { // implements Node interface

	EventCh       chan interface{}
	TimeoutCh     chan interface{}
	CommitChan    chan CommitInfo
	LogHandler    log.Log
	MsgBoxHandler cluster.Server
					   //Term       int64
					   //VoteFor    int64
	Sm            SERVER_DATA
	Cluster       []NetConfig
	LastAlarm     time.Time
	StopSignal    bool
	LogDir        string
}
type CommitInfo struct {

	Data  []byte
	Index int64 // or int .. whatever you have in your code
	Err   error
}

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
func makeRafts(configs []Config) (rafts []RaftNode) {


	for _, element := range configs {

		rafts = append(rafts, startNewRaftNode(element))

	}

	return rafts
}

func startNewRaftNode(config Config) (node *RaftNode) {

	//create state machine in follower mode
	node.initializeStateMachine(config)

	//init the log
	node.initializeLog(config)

	//init messaging framework
	node.initializeMessageBox(config)

	//read log from stored log //to be implemented


	//read term and vote
	node.initializeVoteAndTerm(config)

	//assign the cluster
	//node.Cluster = config.Cluster

	//start election timer
	go node.startTimer()
	go node.processEvents()

	return node
}

func (rn *RaftNode) initializeLog(config Config) {

	rn.LogHandler = log.Open(config.LogDir + "/raftlog")

}

func (rn *RaftNode) initializeMessageBox(config Config) {

	rn.Cluster = config.Cluster
	var err error
	var clConfig cluster.Config
	var prConfig cluster.PeerConfig
	for _, element := range config.Cluster {
		prConfig.Id = element.Id
		prConfig.Address = fmt.Sprintf("%s:%s", element.Host, element.Port)
		clConfig.Peers = append(clConfig.Peers, prConfig)
	}
	rn.MsgBoxHandler, err = cluster.New(rn.Id(), clConfig)

	if err != nil {
		panic(err)
	}

}

func (rn *RaftNode) initializeVoteAndTerm(config Config) {

	fileContent, e := ioutil.ReadFile(config.LogDir + "/termNvote")
	//rn.logHandler = log.Open(config.logDir + "/raftlog")

	if e == nil {

		arr := strings.Split(fileContent, ":")
		rn.Sm.term = arr[0]
		rn.Sm.votedFor = arr[1]

	}else {

		ioutil.WriteFile(config.LogDir + "/termNvote", []byte("0:0"), 0644)
		rn.Sm.term = 0
		rn.Sm.votedFor = 0
	}

}
func (rn *RaftNode) writeVoteAndTerm(state_SM STATE_STORE) {

	ioutil.WriteFile(rn.LogDir + "/termNvote", []byte(fmt.Sprintf("%s:%s", state_SM.term, state_SM.voteFor)), 0644)

}
func (rn *RaftNode) initializeStateMachine(config Config) {

	rn.Sm.candidateId = config.Id
	//svr.leaderId = 3
	//config.cluster
	rn.Sm.candidates = getCandidateIdArray(config)
	//svr.commitIndex =
	rn.Sm.election_time_out = config.ElectionTimeout; //secs
	rn.Sm.heartbeat_time_out = config.HeartbeatTimeout; //secs
	rn.Sm.LOG = make(map[int64]SERVER_LOG_DATASTR)
	//svr.addThisEntry(SERVER_LOG_DATASTR{index:0,term:0,data:[]byte("")}) // dummy log at zeroth index on all servers that are starting up

}

func (rn *RaftNode) startTimer() {

	for {
		//to have a stop mark at shutdown
		//to have a lock for concurrency control

		if time.Now().After(rn.LastAlarm.Add(rn.Sm.heartbeat_time_out * time.Second)) {
			rn.TimeoutCh <- TIMEOUT{}
		}


		//		if rn.Sm.state == LEADER {
		//
		//			if time.Now().After(rn.LastAlarm.Add(rn.Sm.heartbeat_time_out * time.Second)) {
		//				rn.TimeoutCh <- TIMEOUT{}
		//			}
		//		}else {
		//
		//			if time.Now().After(rn.LastAlarm.Add(rn.Sm.election_time_out * time.Second)) {
		//				rn.TimeoutCh <- TIMEOUT{}
		//			}
		//		}

		if rn.StopSignal {
			return
		}

	}
}

func (rn *RaftNode) startListening() {
	for {
		env := <-rn.MsgBoxHandler.Inbox()

		var container interface{}
		json.Unmarshal(env.Msg, &container)


		rn.EventCh <- container

		//to continue from this point.....XXX

		//fmt.Printf("[From: %d MsgId:%d] %s\n", env.Pid, env.MsgId, env.Msg)

		if rn.StopSignal {
			return
		}

	}
}

func getCandidateIdArray(config Config) ([]int64) {

	var cids []int64

	for i := 0; i < len(cids); i++ {
		cids = append(cids, config.Cluster[i])
	}
	return cids
}

func (rn *RaftNode) readLog(config Config) {

	//rn.logHandler = log.Open(config.logDir + "/raftlog")
	//what to do here
}



func (rn *RaftNode) processEvents() {
	var sm SERVER_DATA
	var ev interface{}
	for {

		select {

		case ev <- rn.EventCh:
		case ev <- rn.TimeoutCh:

		}

		actions := sm.processEvent(ev)
		rn.doActions(actions)

		if rn.StopSignal {
			return
		}
	}
}

func (rn *RaftNode) doActions(actions []interface{}) {

	for i := 0; i < len(actions); i++ {
		//to precess each record by type
		select {


		case reflect.TypeOf(actions[i]) == reflect.TypeOf(VOTE_REQUEST{}):

			rn.forwardVoteRequest(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(VOTE_RESPONSE{}):

			rn.forwardVoteResponse(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(APPEND_ENTRIES_REQUEST{}):

			rn.forwardAppendEntriesRequest(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(APPEND_ENTRIES_RESPONSE{}):

			rn.forwardAppendEntriesResponse(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(COMMIT_TO_CLIENT{}):

			rn.forwardToClient(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(SERVER_LOG_DATASTR{}):

			rn.saveToLog(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(RESET_ALARM_ELECTION_TIMEOUT{}):

			rn.setElectionTimeout(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(RESET_ALARM_HEARTBEAT_TIMEOUT{}):

			rn.setHeartbeatTimeout(actions[i])

		case reflect.TypeOf(actions[i]) == reflect.TypeOf(STATE_STORE{}):

			rn.storeTermAndVote(actions[i])

		}
	}

}

func (rn *RaftNode) forwardVoteRequest(vrq_SM VOTE_REQUEST) {

	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: vrq_SM.To_CandidateId, Msg: []byte(json.Marshal(vrq_SM))}


}

func (rn *RaftNode) forwardVoteResponse(vr_SM VOTE_RESPONSE) {

	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: vr_SM.To_CandidateId, Msg: []byte(json.Marshal(vr_SM))}

}
func (rn *RaftNode) forwardAppendEntriesRequest(arq_SM APPEND_ENTRIES_REQUEST) {

	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: arq_SM.To_CandidateId, Msg: []byte(json.Marshal(arq_SM))}


}
func (rn *RaftNode) forwardAppendEntriesResponse(ars_SM APPEND_ENTRIES_RESPONSE) {

	rn.MsgBoxHandler.Outbox() <- &cluster.Envelope{Pid: ars_SM.To_CandidateId, Msg: []byte(json.Marshal(ars_SM))}


}
func (rn *RaftNode) forwardToClient(cmt_to_c COMMIT_TO_CLIENT) {

	rn.CommitChan <- cmt_to_c

}
func (rn *RaftNode) saveToLog(lg_SM SERVER_LOG_DATASTR) {

	rn.LogHandler.TruncateToEnd(lg_SM.Index) //not mandatory
	rn.Append([]byte(json.Marshal(lg_SM)))

}
func (rn *RaftNode) setElectionTimeout(vr_SM RESET_ALARM_ELECTION_TIMEOUT) {

	rn.LastAlarm = time.Now()

}
func (rn *RaftNode) setHeartbeatTimeout(vr_SM RESET_ALARM_HEARTBEAT_TIMEOUT) {

	rn.LastAlarm = time.Now()

}
func (rn *RaftNode) storeTermAndVote(st_SM STATE_STORE) {

	rn.writeVoteAndTerm(st_SM)

}

//********************************************INTERFACE_METHODS******************************************
func (rn *RaftNode) Append(data []byte) {

	rn.EventCh <- data

}
func (rn *RaftNode) CommitChannel(data []byte) (cmt CommitInfo) {

	cmt <- rn.CommitChan
	return cmt
}
//to implement locking here
func (rn *RaftNode) CommittedIndex() (ci int64) {

	return rn.Sm.commitIndex
}
func (rn *RaftNode) GetIndex(ci int64) ([]byte, error) {

	return rn.LogHandler.Get(ci)
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
}

//********************************************************************************************************
