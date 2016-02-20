package main
import "math/rand"

//all structures

const (
	CANDIDATE = 0
	LEADER = 1
	FOLLOWER = 2
	NONE = -1  //const for votedfor=nil
	ELECTION_TIMEOUT_VALUE = 30
	HEARTBEAT_TIMEOUT_VALUE = 20
	APPEND_DATA = "[]uint8"

)


//primary server data
type SERVER_DATA struct {

	term                   int64
	candidateId            int64
	candidates             []int64

	election_time_out      int64
	heartbeat_time_out     int64                        //used as a marker for map length
	lastLogIndex           int64
	commitIndex            int64
	leaderId               int64
	state                  int8
	votedFor               int64
	LOG                    map[int64]SERVER_LOG_DATASTR //needs initialization //here key is the index

	//assumption that whena server starts fresh at data center establishent then it contains a log at index 0 with {term:0,index:0,data:x}
	//this ensures that the first ever log append to any server will succeed as the base log is same for all

	leaderStateAttrData    SERVER_LEADER_DATA //leader attr
	candidateStateAttrData SERVER_CANDIDATE_DATA //candidate attr

}
//attributes that are relevant when server is a leader
type SERVER_LEADER_DATA struct {
	nextIndex  map[int64]SERVER_LOG_DATASTR //here key is the candidate ID , the values are states of other candidates
	matchIndex map[int64]SERVER_LOG_DATASTR //here key is the candidate ID , the values are states of other candidates
}
//attributes that are relevant when server is a candidate

type SERVER_CANDIDATE_DATA struct {
	positiveVoteCount int64
	negativeVoteCount int64
	voteTerm  int64
}

//in this section we define different packet request response structures

// data structure for a single log of the server log map
//this is also used for the purpose  of a log store msg structure to upper layer
type SERVER_LOG_DATASTR struct {
	isHeartbeat bool //false by default
	term        int64
	index       int64
	data        []byte
}

//vote request msg structure

type VOTE_REQUEST struct {

	electionTerm     int64
	from_CandidateId int64
	to_CandidateId   int64
	myLastLog        SERVER_LOG_DATASTR //needs initialization //here key is the index

}

//vote response msg structure
type VOTE_RESPONSE struct {

	responderTerm    int64
	from_CandidateId int64
	to_CandidateId   int64
	voted            bool
}
//append entries msg structure

type APPEND_ENTRIES_REQUEST struct {
	leaderCommitIndex int64
	leaderTerm        int64
	from_CandidateId  int64
	to_CandidateId    int64
	logToAdd          SERVER_LOG_DATASTR
	leaderLastLog     SERVER_LOG_DATASTR //needs initialization //here key is the index

}
// append entry response msg structure
type APPEND_ENTRIES_RESPONSE struct {

	responderTerm    int64
	from_CandidateId int64
	to_CandidateId   int64
	appendSuccess    bool
}
// client commit msg structure which will be sent to upper layer
type COMMIT_TO_CLIENT struct {
	index    int64
	data []byte
	err_code   int64
}
//election time out msg str
type RESET_ALARM_ELECTION_TIMEOUT struct {
	duration    int64
}
// heartbeat timeout msg structure
type RESET_ALARM_HEARTBEAT_TIMEOUT struct {
	duration    int64
}
// no action msg structure, dummy str can be used when we dont have any action but wants to send a response
type NO_ACTION struct {
}

//timeout alarm msg structure
//recognise a timeout event type at the switch case level by this dummy parameter
type TIMEOUT struct {
}

//append data redirection from follower to leader
type REDIRECT_APPEND_DATA struct {

	data []byte
	redirectToiId int64
}
//state store msg str
type STATE_STORE struct {

	term int64
	voteFor int64
}

//checks lg equality
func isEqual(log1 SERVER_LOG_DATASTR,log2 SERVER_LOG_DATASTR)(ret bool){

	if log1.index == log2.index && log1.term == log2.term {
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

	if thisServer.LOG[thisServer.lastLogIndex].term < incmngReq.myLastLog.term {
		resp = true
	}else if thisServer.LOG[thisServer.lastLogIndex].term == incmngReq.myLastLog.term {
		if thisServer.LOG[thisServer.lastLogIndex].index <= incmngReq.myLastLog.index   {
			resp = true
		}else {
			resp = false
		}
	}else{ //server lastlog term > incoming req last log term
	resp = false
	}

	return resp
}

func (thisServer *SERVER_DATA) SomeLastLogIsSameAs(incmngReq APPEND_ENTRIES_REQUEST) (resp bool) {


	if thisServer.LOG[incmngReq.leaderLastLog.index].term == incmngReq.leaderLastLog.term {
		resp = true
		thisServer.lastLogIndex = incmngReq.leaderLastLog.index
	}else {
		resp = false
	}

	return resp
}

//func (thisServer *SERVER_DATA) LastLogIsSa (incmngReq APPEND_ENTRIES_REQUEST)(resp bool){
//
//	if thisServer.LOG[thisServer.lastLogIndex].index <= incmngReq.logToAdd.index && thisServer.LOG[thisServer.lastLogIndex].term <= incmngReq.logToAdd.term {
//		resp = true
//	}else{
//		resp = false
//	}
//
//	return resp
//}

//same term check
func (thisServer *SERVER_DATA) onSameTermWith(incmngReq VOTE_REQUEST) (resp bool) {

	if thisServer.term == incmngReq.electionTerm {
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
func (thisServer *SERVER_DATA) randomizeTimeout() {

	thisServer.election_time_out = int64(rand.Float32() * float32(thisServer.election_time_out))
}


//log adding function to add as next log ntry and increase last log index
func (thisServer *SERVER_DATA) addThisEntry(log SERVER_LOG_DATASTR) {

	thisServer.lastLogIndex ++;
	thisServer.LOG[thisServer.lastLogIndex] = log

}
//add this at index position
func (thisServer *SERVER_DATA) addThisLogEntry(log SERVER_LOG_DATASTR) {

	thisServer.lastLogIndex = log.index
	thisServer.LOG[thisServer.lastLogIndex] = log
}




//voteresponse

func makeReq(thisServer *SERVER_DATA, id int64) (VOTE_REQUEST) {
	var votereq VOTE_REQUEST


	votereq.electionTerm = thisServer.term
	votereq.from_CandidateId = thisServer.candidateId
	votereq.to_CandidateId = id
	votereq.myLastLog = thisServer.LOG[thisServer.lastLogIndex]

	return votereq
}

func (voteres *VOTE_RESPONSE) makeResp(thisServer *SERVER_DATA, incmgreq VOTE_REQUEST, votedecision bool) {

	voteres.from_CandidateId = thisServer.candidateId
	voteres.responderTerm = thisServer.term
	voteres.voted = votedecision
	voteres.to_CandidateId = incmgreq.from_CandidateId
}
//used only for hearbeats
func makeAppendEntryReq(thisServer *SERVER_DATA, sendId int64, isHeartBeat bool) (APPEND_ENTRIES_REQUEST) {

	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.leaderTerm = thisServer.term
	appendentrReqObject.from_CandidateId = thisServer.candidateId
	appendentrReqObject.leaderCommitIndex = thisServer.commitIndex
	appendentrReqObject.to_CandidateId = sendId
	appendentrReqObject.leaderLastLog = thisServer.LOG[thisServer.lastLogIndex]

	//var testlog SERVER_LOG_DATASTR

	//testlog.makeLog(1,1,[]byte("cmd1"))

	if isHeartBeat {
		appendentrReqObject.logToAdd.isHeartbeat = isHeartBeat
	}

	return appendentrReqObject
}

func (appres *APPEND_ENTRIES_RESPONSE) makeResp(thisServer *SERVER_DATA, incmgreq APPEND_ENTRIES_REQUEST, appendDec bool) {

	appres.from_CandidateId = thisServer.candidateId
	appres.responderTerm = thisServer.term
	appres.appendSuccess = appendDec
	appres.to_CandidateId = incmgreq.from_CandidateId
}
func (alog *SERVER_LOG_DATASTR) makeLog(index int64, term int64, data []byte) {
	alog.index = index
	alog.term = term
	alog.data = data

}
