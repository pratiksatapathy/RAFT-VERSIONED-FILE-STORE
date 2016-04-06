package main
import (
	"sync"
	"github.com/cs733-iitb/log"
)

//all structures

const (
	CANDIDATE = 0
	LEADER = 1
	FOLLOWER = 2
	UNDEF = -5
	NONE = -1  //const for votedfor=nil
	ELECTION_TIMEOUT_VALUE = 30
	HEARTBEAT_TIMEOUT_VALUE = 20
	APPEND_DATA = "[]uint8"
	ERR_NOT_LEADER = 100

)


//primary server data
type SERVER_DATA struct {

	term                   int64
	candidateId            int64
	candidates             []int64

	MutX_SM                *sync.Mutex

	election_time_out      int64
	heartbeat_time_out     int64                 //used as a marker for map length
												 //lastLogIndex           int64
	commitIndex            int64
	leaderId               int64
	state                  int8
	votedFor               int64
	temp                   int64
	LogHandler             *log.Log
												 //LOG                    map[int64]SERVER_LOG_DATASTR //needs initialization //here key is the index

												 //assumption that whena server starts fresh at data center establishent then it contains a log at index 0 with {term:0,index:0,data:x}
												 //this ensures that the first ever log append to any server will succeed as the base log is same for all

	leaderStateAttrData    SERVER_LEADER_DATA    //leader attr
	candidateStateAttrData SERVER_CANDIDATE_DATA //candidate attr
	appendSynch int64

}
//attributes that are relevant when server is a leader
type SERVER_LEADER_DATA struct {
	nextIndex   []int64                      //here key is the candidate ID , the values are states of other candidates
	matchIndex  map[int64]SERVER_LOG_DATASTR //here key is the candidate ID , the values are states of other candidates

	lastNext []int64

}
//attributes that are relevant when server is a candidate

type SERVER_CANDIDATE_DATA struct {
	positiveVoteCount int64
	negativeVoteCount int64
	voteTerm          int64
}

//in this section we define different packet request response structures

// data structure for a single log of the server log map
//this is also used for the purpose  of a log store msg structure to upper layer
type SERVER_LOG_DATASTR struct {

	Term  int64
	Index int64
	Data  []byte
}

//vote request msg structure

type VOTE_REQUEST struct {

	ElectionTerm     int64
	From_CandidateId int64
	To_CandidateId   int64
	MyLastLog        SERVER_LOG_DATASTR //needs initialization //here key is the index

}

//vote response msg structure
type VOTE_RESPONSE struct {

	ResponderTerm    int64
	From_CandidateId int64
	To_CandidateId   int64
	Voted            bool
}
//append entries msg structure

type APPEND_ENTRIES_REQUEST struct {
	LeaderCommitIndex int64
	LeaderTerm        int64
	From_CandidateId  int64
	To_CandidateId    int64
	IsHeartbeat       bool               //false by default
	LogToAdd          SERVER_LOG_DATASTR
	MoreLog           []SERVER_LOG_DATASTR
	LeaderLastLog     SERVER_LOG_DATASTR //needs initialization //here key is the index
	msgid	int64

}
// append entry response msg structure
type APPEND_ENTRIES_RESPONSE struct {

	ResponderTerm     int64
	From_CandidateId  int64
	To_CandidateId    int64
	AppendSuccess     bool
	appendedIndexFrom int64
	appendedIndexTo   int64


}
// client commit msg structure which will be sent to upper layer
type COMMIT_TO_CLIENT struct {
	Index    int64
	Data     []byte
	Err_code int64
}
//election time out msg str
type RESET_ALARM_ELECTION_TIMEOUT struct {
	duration int64
}
// heartbeat timeout msg structure
type RESET_ALARM_HEARTBEAT_TIMEOUT struct {
	duration int64
}
// no action msg structure, dummy str can be used when we dont have any action but wants to send a response
type NO_ACTION struct {
}

//timeout alarm msg structure
//recognise a timeout event type at the switch case level by this dummy parameter
type TIMEOUT struct {
}

//append data redirection from follower to leader
type REDIRECT_APPEND_DATA struct { //not required anymore

	data          []byte
	redirectToiId int64
}
//state store msg str
type STATE_STORE struct {

	term    int64
	voteFor int64
}

//checks lg equality
func isEqual(log1 SERVER_LOG_DATASTR, log2 SERVER_LOG_DATASTR) (ret bool) {

	if log1.Index == log2.Index && log1.Term == log2.Term {
		ret = true
	}
	return ret
}

//returns the count of majority at any instance by reading candidates array
func majorityCount(candidates []int64) (majority int64) {

	if len(candidates) % 2 == 0 {
		majority = int64(len(candidates) / 2 + 1)
	}else {
		majority = int64((len(candidates) + 1) / 2)
	}
	return majority
}

//checks if the log is not more updated than
func (thisServer *SERVER_DATA) LogIsNotMoreUpdatedThan(incmngReq VOTE_REQUEST) (resp bool) {

	if (thisServer.getLogAtIndex(thisServer.LogHandler.GetLastIndex())).Term < incmngReq.MyLastLog.Term {
		resp = true
	}else if (thisServer.getLogAtIndex(thisServer.LogHandler.GetLastIndex())).Term == incmngReq.MyLastLog.Term {
		if (thisServer.getLogAtIndex(thisServer.LogHandler.GetLastIndex())).Index <= incmngReq.MyLastLog.Index {
			resp = true
		}else {
			resp = false
		}
	}else { //server lastlog term > incoming req last log term
		resp = false
	}

	return resp
}

func (thisServer *SERVER_DATA) SomeLastLogIsSameAs(incmngReq APPEND_ENTRIES_REQUEST) (resp bool) {

	//thisServer.debug_output2("server56",thisServer.LOG)
	thisServer.debug_output2("req", incmngReq)

	//fmt.Println("===============================")
	if (thisServer.getLogAtIndex(incmngReq.LeaderLastLog.Index)).Term == incmngReq.LeaderLastLog.Term {
		resp = true
		//thisServer.lastLogIndex = incmngReq.LeaderLastLog.Index
	}else {
		resp = false
	}

	thisServer.debug_output2("req response:..", resp)
	return resp
}

//func (thisServer *SERVER_DATA) LastLogIsSa (incmngReq APPEND_ENTRIES_REQUEST)(resp bool){
//
//	if thisServer.LOG[thisServer.lastLogIndex].index <= incmngReq.logToAdd.index && thisServer.LOG[thisServer.lastLogIndex].Term <= incmngReq.logToAdd.Term {
//		resp = true
//	}else{
//		resp = false
//	}
//
//	return resp
//}

//same term check
func (thisServer *SERVER_DATA) onSameTermWith(incmngReq VOTE_REQUEST) (resp bool) {

	if thisServer.term == incmngReq.ElectionTerm {
		resp = true
	}else {
		resp = false
	}

	return resp
}

func (thisServer *SERVER_DATA) hasNotVoted() (resp bool) {

	if thisServer.votedFor == NONE {
		resp = true
	}else {
		resp = false
	}

	return resp
}

//rndomizing timeouts
//func (thisServer *SERVER_DATA) randomizeTimeout() {
//
//	a := int64(rand.Float32() * float32(thisServer.election_time_out))
//	a = a
//	//thisServer.election_time_out = int64(rand.Float32() * float32(thisServer.election_time_out))
//}


//log adding function to add as next log ntry and increase last log index
func (thisServer *SERVER_DATA) addThisEntries(logs []SERVER_LOG_DATASTR) {

	for i := 0; i < len(logs); i++ {

		log := logs[i]

		if log.Index > thisServer.commitIndex {

			//thisServer.lastLogIndex  = log.Index
			thisServer.LogHandler.TruncateToEnd(log.Index)
			thisServer.LogHandler.Append(log)

		}
	}

}
func (thisServer *SERVER_DATA) addThisEntry(log SERVER_LOG_DATASTR) {

	if log.Index > thisServer.commitIndex {
		thisServer.LogHandler.TruncateToEnd(log.Index)
		thisServer.LogHandler.Append(log)

	}


}
//add this at index position
func (thisServer *SERVER_DATA) addThisLogEntry(log SERVER_LOG_DATASTR) {

	//thisServer.lastLogIndex = log.Index
	thisServer.LogHandler.Append(log)
}
func (thisServer *SERVER_DATA) lastLog() (SERVER_LOG_DATASTR) {

	log, err := thisServer.LogHandler.Get(thisServer.LogHandler.GetLastIndex())
	//thisServer.lastLogIndex = log.Index
	//thisServer.append([]byte(log))
	check(err)
	return log.(SERVER_LOG_DATASTR)
}
func (thisServer *SERVER_DATA) lastCommitLog() (SERVER_LOG_DATASTR) {

	log, err := thisServer.LogHandler.Get(thisServer.commitIndex)
	//thisServer.lastLogIndex = log.Index
	//thisServer.append([]byte(log))
	check(err)
	return log.(SERVER_LOG_DATASTR)
}
func (thisServer *SERVER_DATA) getLogAtIndex(index int64) (SERVER_LOG_DATASTR) {

	//fmt.Print("getting:");fmt.Print(index);fmt.Print(thisServer.candidateId)
	log, _ := thisServer.LogHandler.Get(index)
	//fmt.Print("%v/////",log)
	if log == nil {
		return SERVER_LOG_DATASTR{Index:-1}
	}
	//thisServer.lastLogIndex = log.Index
	//thisServer.append([]byte(log))

	//check(err)
	return log.(SERVER_LOG_DATASTR)
}
func (thisServer *SERVER_DATA) getLogFromIndex(index int64) ([]SERVER_LOG_DATASTR) {

	logs := make([]SERVER_LOG_DATASTR, 0)

	if index <= thisServer.LogHandler.GetLastIndex() {

		for i := index; i <= thisServer.LogHandler.GetLastIndex(); i++ {

			logs = append(logs, thisServer.getLogAtIndex(i))
		}
	}
	return logs
}




//voteresponse

func makeReq(thisServer *SERVER_DATA, id int64) (VOTE_REQUEST) {
	var votereq VOTE_REQUEST


	votereq.ElectionTerm = thisServer.term
	votereq.From_CandidateId = thisServer.candidateId
	votereq.To_CandidateId = id

	log, err := thisServer.LogHandler.Get(thisServer.LogHandler.GetLastIndex())
	check(err)
	votereq.MyLastLog = log.(SERVER_LOG_DATASTR)
	return votereq
}

func (voteres *VOTE_RESPONSE) makeResp(thisServer *SERVER_DATA, incmgreq VOTE_REQUEST, votedecision bool) {

	voteres.From_CandidateId = thisServer.candidateId
	voteres.ResponderTerm = thisServer.term
	voteres.Voted = votedecision
	voteres.To_CandidateId = incmgreq.From_CandidateId
}
//used only for hearbeats
func makeAppendEntryReq(thisServer *SERVER_DATA, sendId int64, isHeartBeat bool) (APPEND_ENTRIES_REQUEST) {

	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = thisServer.term
	appendentrReqObject.From_CandidateId = thisServer.candidateId
	appendentrReqObject.LeaderCommitIndex = thisServer.commitIndex
	appendentrReqObject.To_CandidateId = sendId

	log, err := thisServer.LogHandler.Get(thisServer.LogHandler.GetLastIndex())
	check(err)
	appendentrReqObject.LeaderLastLog = log.(SERVER_LOG_DATASTR)

	//var testlog SERVER_LOG_DATASTR

	//testlog.makeLog(1,1,[]byte("cmd1"))

	if isHeartBeat {
		appendentrReqObject.IsHeartbeat = isHeartBeat
	}

	return appendentrReqObject
}

func (appres *APPEND_ENTRIES_RESPONSE) makeResp(thisServer *SERVER_DATA, incmgreq APPEND_ENTRIES_REQUEST, appendDec bool) {

	//fmt.Println("line384",incmgreq.LogToAdd,"HB:",incmgreq.IsHeartbeat)
	appres.From_CandidateId = thisServer.candidateId
	appres.ResponderTerm = thisServer.term
	appres.AppendSuccess = appendDec
	appres.To_CandidateId = incmgreq.From_CandidateId
	appres.appendedIndexFrom = incmgreq.LogToAdd.Index

//	if len(incmgreq.MoreLog) > 0 {
//		appres.appendedIndexTo = incmgreq.MoreLog[len(incmgreq.MoreLog)-1].Index
//	}else{
		appres.appendedIndexTo = incmgreq.LogToAdd.Index

	//}

}
func (alog *SERVER_LOG_DATASTR) makeLog(index int64, term int64, data []byte) {
	alog.Index = index
	alog.Term = term
	alog.Data = data

}

