package main

import (
	"testing"
	"reflect"
)
//utility function tests
func TestMajorityCount(t *testing.T) {

	//test for majority count check
	if majorityCount([]int64{5, 2, 2, 3, 4, 1}) != 4 {
		t.Error("Expexted 4")
	}
}

func TestLogIsNotMoreUpdatedThanFunction(t *testing.T) {
	var svr SERVER_DATA
	svr.setBasicMachine()


	//TEST STATE SETUP

	svr.state = CANDIDATE
	svr.term = 2;
	svr.lastLogIndex = 1

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.votedFor = svr.candidateId

	//preparing a vote request
	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 2
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}

	if svr.LogIsNotMoreUpdatedThan(voteReqObject) != true {

		t.Error("should be true")
	}
//--------------------------------------------------------------------------------
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd1")})

	voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}

	if svr.LogIsNotMoreUpdatedThan(voteReqObject) != false {

		t.Error("should be false")
	}
//--------------------------------------------------------------------------------------
	voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:4,Term:4,Data:[]byte("cmd1")}

	if svr.LogIsNotMoreUpdatedThan(voteReqObject) != true {

		t.Error("should be true")
	}

//----------------------------------------------------------------------------------
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:4,Term:2,Data:[]byte("cmd1")})
	voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd1")}

	if svr.LogIsNotMoreUpdatedThan(voteReqObject) != false {

		t.Error("should be false")
	}



}
//basic  state assignment , common for all tests

func (svr *SERVER_DATA)setBasicMachine() {

	//other node informations
	svr.candidateId = 1
	svr.leaderId = 3
	svr.candidates = []int64{1, 2, 3, 4, 5}
	//svr.commitIndex =
	svr.election_time_out = ELECTION_TIMEOUT_VALUE; //secs
	svr.heartbeat_time_out = HEARTBEAT_TIMEOUT_VALUE; //secs
	svr.LOG = make(map[int64]SERVER_LOG_DATASTR)
	svr.addThisEntry(SERVER_LOG_DATASTR{Index:0,Term:0,Data:[]byte("")}) // dummy log at zeroth index on all servers that are starting up


}
//test for invalid data to process event
func TestInvalidEntry(t *testing.T) {

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	ret:= svr.processEvent(NONE)
	if reflect.TypeOf(ret[0]) != reflect.TypeOf(NO_ACTION{}) {

	t.Error("no action expected")
	}
}

func TestVoteRequestToFollower1(t *testing.T) { //give vote on new term request


	//BASIC MACHINE SETUP

	var svr SERVER_DATA
	svr.setBasicMachine()
	svr.state = FOLLOWER;
	svr.term = 1;
	svr.lastLogIndex = 1

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.votedFor = NONE

	//PREPARE EVENT

	testlog:= SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}
	voteReqObject := VOTE_REQUEST{ElectionTerm:2,From_CandidateId:3,MyLastLog:testlog}


	//CALL AND CHECK


	ret := svr.processEvent(voteReqObject)
	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	var voteResp VOTE_RESPONSE
	voteResp = (ret[1]).(VOTE_RESPONSE)

	if 	voteResp.Voted == true &&
		voteResp.From_CandidateId == 1 &&
		voteResp.To_CandidateId == 3 {

	}else {
		t.Error(voteResp)
	}
}

func TestVoteRequestToFollower2(t *testing.T) { //deny vote on already vote case

//BASIC MACHINE SETUP

	var svr SERVER_DATA
	svr.setBasicMachine()


	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 1;
	svr.lastLogIndex = 1
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.votedFor = 4


	//preparing a vote request
	testlog := SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}

	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 1
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog = testlog

	voteResp := ((svr.processEvent(voteReqObject))[0]).(VOTE_RESPONSE)

	if 	voteResp.Voted == false &&
	 	voteResp.From_CandidateId == 1 &&
	 	voteResp.To_CandidateId == 3 {
		//this is expected
	}else {
		t.Error(voteResp)
	}
}

func TestVoteRequestToFollower3(t *testing.T) { //deny vote on old term vote request

var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 2;
	svr.lastLogIndex = 0

	svr.LOG = make(map[int64]SERVER_LOG_DATASTR)
	svr.votedFor = NONE

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	//preparing a vote request

	testlog:= SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}
	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 1
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog = testlog

	voteResp := ((svr.processEvent(voteReqObject))[0]).(VOTE_RESPONSE)

	if 	voteResp.Voted == false &&
	 	voteResp.From_CandidateId == 1 &&
	 	voteResp.To_CandidateId == 3 {
		//this is expected
	}else {
		t.Error("wrong voted at testVoterequest3")
	}
}

func TestVoteRequestToFollower4(t *testing.T) { //deny vote on old term vote request

var svr SERVER_DATA
svr.setBasicMachine()

//TEST STATE SETUP

svr.state = FOLLOWER;
svr.term = 2;
svr.lastLogIndex = 0

svr.LOG = make(map[int64]SERVER_LOG_DATASTR)
svr.votedFor = NONE

svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

//preparing a vote request

var voteReqObject VOTE_REQUEST
voteReqObject.ElectionTerm = 2
voteReqObject.From_CandidateId = 3
voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}

voteResp := ((svr.processEvent(voteReqObject))[0]).(VOTE_RESPONSE)

if 	voteResp.Voted == false &&
voteResp.From_CandidateId == 1 &&
voteResp.To_CandidateId == 3 {
//this is expected
}else {
t.Error("wrong voted at testVoterequest3")
}
}
func TestVoteRequestToFollower5(t *testing.T) { //agree to vote as log is identical

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 2;
	svr.lastLogIndex = 0

	svr.LOG = make(map[int64]SERVER_LOG_DATASTR)
	svr.votedFor = NONE

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})

	//preparing a vote request

	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 2
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}

	ret := svr.processEvent(voteReqObject)
	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error(ret[0])
	}
	voteResp := (ret[1]).(VOTE_RESPONSE)

	if 	voteResp.Voted == true &&
	voteResp.From_CandidateId == 1 &&
	voteResp.To_CandidateId == 3 {
		//this is expected
	}else {
		t.Error("wrong voted at testVoterequest3")
	}
}

func TestVoteRequestToFollower6(t *testing.T) { //term of incoming req is higher but in log the term of last logs are same , svr log is longer

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 2;
	svr.lastLogIndex = 0

	svr.LOG = make(map[int64]SERVER_LOG_DATASTR)
	svr.votedFor = NONE

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd1")})

	//preparing a vote request

	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 3
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}

	ret := svr.processEvent(voteReqObject)
	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error(ret[0])
	}
	voteResp := (ret[1]).(VOTE_RESPONSE)

	if 	voteResp.Voted == false &&
	voteResp.From_CandidateId == 1 &&
	voteResp.To_CandidateId == 3 {
		//this is expected
	}else {
		t.Error("wrong voted at testVoterequest3")
	}
}

func TestVoteRequestToCandidate1(t *testing.T) { //deny vote on already voted case


//BASIC MACHINE SETUP

	var svr SERVER_DATA
	svr.setBasicMachine()


	//TEST STATE SETUP

	svr.state = CANDIDATE
	svr.term = 2;
	svr.lastLogIndex = 1

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.votedFor = svr.candidateId

	//preparing a vote request
	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 2
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}


	voteResp := ((svr.processEvent(voteReqObject))[0]).(VOTE_RESPONSE) //assertion

	if 	voteResp.Voted == false &&
	 	voteResp.From_CandidateId == 1 &&
	 	voteResp.To_CandidateId == 3 &&
	 	svr.state == CANDIDATE {
		//this is expected
	}else {
		t.Error(voteResp)
	}
}

func TestVoteRequestToCandidate2(t *testing.T) { //accept vote from a newer term

//BASIC MACHINE SETUP

	var svr SERVER_DATA
	svr.setBasicMachine()


	//TEST STATE SETUP

	svr.state = CANDIDATE;
	svr.term = 1;
	svr.lastLogIndex = 1

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.votedFor = svr.candidateId

	//PREPARE EVENT
	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 3
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog  = SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd1")}


	ret := svr.processEvent(voteReqObject)
	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

voteResp := (ret[1]).(VOTE_RESPONSE) //assertion

	if 	voteResp.Voted == true &&
		voteResp.From_CandidateId == 1 &&
		voteResp.To_CandidateId == 3 &&
		svr.state == FOLLOWER {

		//this is expected
	}else {
		t.Error(voteResp)
	}
}
func TestVoteRequestToCandidate3(t *testing.T) { //term is same but candidate is more updated than the incoming request

	//BASIC MACHINE SETUP

	var svr SERVER_DATA
	svr.setBasicMachine()


	//TEST STATE SETUP

	svr.state = CANDIDATE;
	svr.term = 1;
	svr.lastLogIndex = 0

	svr.addThisEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd1")})
	svr.addThisEntry(SERVER_LOG_DATASTR{Index:3,Term:2,Data:[]byte("cmd1")})
	svr.debug_output2("lastlog\n",svr.lastLogIndex)
	svr.votedFor = svr.candidateId

	//PREPARE EVENT
	var voteReqObject VOTE_REQUEST
	voteReqObject.ElectionTerm = 3
	voteReqObject.From_CandidateId = 3
	voteReqObject.MyLastLog  = SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd1")}


	ret := svr.processEvent(voteReqObject)
	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	voteResp := (ret[1]).(VOTE_RESPONSE) //assertion

	if 	voteResp.Voted == false &&
	voteResp.From_CandidateId == 1 &&
	voteResp.To_CandidateId == 3 &&
	svr.state == CANDIDATE {

		//this is expected
	}else {
		t.Error(voteResp)
	}
}

func TestAppendEntriesRequestToFollower(t *testing.T) { //reject entries from an older term


var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 2;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	//preparing event
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 1
	appendentrReqObject.From_CandidateId = 3

	appendentrReqObject.LeaderLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}
	appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd1")}

	ret:=svr.processEvent(appendentrReqObject)
	appentResp := (ret[0]).(APPEND_ENTRIES_RESPONSE) //assertion

	if 	appentResp.AppendSuccess == false &&
		appentResp.From_CandidateId == 1 &&
		appentResp.To_CandidateId == 3 &&
		svr.state == FOLLOWER {

		//this is expected
	}else {
		t.Error(appentResp)
	}



}


func TestAppendEntriesRequestToFollower2(t *testing.T) { //send heartbeat entries and expect the parameter 30 in alert(30) in the timeout channel

var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 2;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})


	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 2
	appendentrReqObject.From_CandidateId = 3
	//appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:1,Term:2,Data:[]byte("cmd1")}
	appendentrReqObject.IsHeartbeat = true
	//fmt.Print("here=");fmt.Print(appendentrReqObject.LogToAdd)

	ret:=svr.processEvent(appendentrReqObject)
	timeoutResp := ret[0].(RESET_ALARM_ELECTION_TIMEOUT) //assertion

	if timeoutResp.duration == 30 {
		//this is expected
	}else {
		t.Error(timeoutResp)
	}
}

func TestAppendEntriesRequestToFollower3(t *testing.T) { //accpet entries from a leader after last log match

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 2;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 2
	appendentrReqObject.From_CandidateId = 3

	appendentrReqObject.LeaderLastLog = SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")}
	appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:2,Term:3,Data:[]byte("cmd3")}

	ret:=svr.processEvent(appendentrReqObject);

	log := (ret[0]).(SERVER_LOG_DATASTR) //assertion
	if 	log.Index == 2 &&
	log.Term == 3{
		//this is expected
	}else {
		t.Error(log)
	}
	appentResp := (ret[1]).(APPEND_ENTRIES_RESPONSE) //assertion
	if 	appentResp.AppendSuccess == true &&
	 	appentResp.From_CandidateId == 1 &&
	  	appentResp.To_CandidateId == 3 &&
	   	svr.state == FOLLOWER {
		//this is expected
	}else {
		t.Error(appentResp)
	}
	electtimeoutreset := (ret[2]).(RESET_ALARM_ELECTION_TIMEOUT)

	if electtimeoutreset.duration != svr.election_time_out{
		t.Error(electtimeoutreset)
	}
}

func TestAppendEntriesRequestToFollower4(t *testing.T) { //lastlog didnt match send back false status

	var svr SERVER_DATA
	svr.setBasicMachine()

svr.state = FOLLOWER;
	svr.term = 1;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:1,Data:[]byte("cmd3")})

	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 3
	appendentrReqObject.From_CandidateId = 3

	appendentrReqObject.LeaderLastLog = SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd1")}
	appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:3,Term:3,Data:[]byte("cmd2")}

	ret:= svr.processEvent(appendentrReqObject);

	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	appentResp := (ret[1]).(APPEND_ENTRIES_RESPONSE) //assertion

	if 	appentResp.AppendSuccess == false &&
	 	appentResp.From_CandidateId == 1 &&
	  	appentResp.To_CandidateId == 3 &&
	   	svr.state == FOLLOWER {
		//this is expected
	}else {
		t.Error(appentResp)
	}

	electtimeoutreset := (ret[2]).(RESET_ALARM_ELECTION_TIMEOUT)

	if electtimeoutreset.duration != svr.election_time_out{
		t.Error(electtimeoutreset)
	}
}

func TestAppendEntriesRequestToFollower5(t *testing.T) { //accpet entries from a leader after 2nd log match and replace the third log entry

	var svr SERVER_DATA
	svr.setBasicMachine()

svr.state = FOLLOWER;
	svr.term = 1;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:1,Data:[]byte("cmd3")})

	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 2
	appendentrReqObject.From_CandidateId = 3

	var testlog SERVER_LOG_DATASTR
	testlog.makeLog(2, 1, []byte("cmd1")) //leader last log  (index,term,data)
	appendentrReqObject.LeaderLastLog = testlog

	var testlog2 SERVER_LOG_DATASTR
	testlog2.makeLog(3, 2, []byte("cmd2"))  //logtoadd  (index,term,data)
	appendentrReqObject.LogToAdd = testlog2

	ret:=svr.processEvent(appendentrReqObject);

	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	log := (ret[1]).(SERVER_LOG_DATASTR) //assertion
	if 	log.Index == 3 &&
	log.Term == 2{
		//this is expected
	}else {
		t.Error(log)
	}
	appentResp := (ret[2]).(APPEND_ENTRIES_RESPONSE) //assertion
	if 	appentResp.AppendSuccess == true &&
	appentResp.From_CandidateId == 1 &&
	appentResp.To_CandidateId == 3 &&
	svr.state == FOLLOWER {
		//this is expected
	}else {
		t.Error(appentResp)
	}
	electtimeoutreset := (ret[3]).(RESET_ALARM_ELECTION_TIMEOUT)

	if electtimeoutreset.duration != svr.election_time_out{
		t.Error(electtimeoutreset)
	}
}

func TestAppendEntriesRequestToCandidate1(t *testing.T) { //accpet entries from a leader after 2nd log match and replace the third log entry[converts to follower]

	var svr SERVER_DATA
		svr.setBasicMachine()

	svr.state = CANDIDATE;
		svr.term = 1;
		svr.lastLogIndex = 1
		svr.votedFor = svr.candidateId

		svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
		svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd2")})
		svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:1,Data:[]byte("cmd3")})

	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 2
	appendentrReqObject.From_CandidateId = 3

	appendentrReqObject.LeaderLastLog = SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd1")}
	appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:3,Term:2,Data:[]byte("cmd2")}

	ret:= svr.processEvent(appendentrReqObject);

	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	//
	log := (ret[1]).(SERVER_LOG_DATASTR) //assertion
	if 	log.Index == 3 &&
	log.Term == 2{
		//this is expected
	}else {
		t.Error(log)
	}

	//


	appentResp := (ret[2]).(APPEND_ENTRIES_RESPONSE) //assertion

	if appentResp.AppendSuccess == true &&
		appentResp.From_CandidateId == 1 &&
		appentResp.To_CandidateId == 3 &&
		svr.state == FOLLOWER && svr.lastLogIndex == 3 &&
		svr.LOG[svr.lastLogIndex].Term == 2 {
		//this is expected
	}else {
		t.Error("TestAppendEntriesRequestTocandidate1 %d", svr.lastLogIndex)
	}
}

func TestAppendEntriesRequestToCandidate2(t *testing.T) { //reject entries from an older term and behaves as candidate


var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = CANDIDATE;
	svr.term = 2;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 1
	appendentrReqObject.From_CandidateId = 3


	appendentrReqObject.LeaderLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}
	appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd1")}

	ret:=svr.processEvent(appendentrReqObject);

	appentResp := (ret[0]).(APPEND_ENTRIES_RESPONSE) //assertion
	if appentResp.AppendSuccess == false &&
	appentResp.From_CandidateId == 1 &&
	appentResp.To_CandidateId == 3 &&
	svr.state == CANDIDATE {
		//this is expected
	}else {
		t.Error("wrong voted at TestAppendEntriescandidate2")
	}

}

func TestAppendEntriesRequestToLeader1(t *testing.T) { //accpet entries from a high term leader [converts to follower]

var svr SERVER_DATA
	svr.setBasicMachine()

svr.state = LEADER;
	svr.term = 1;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:1,Data:[]byte("cmd3")})

	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 2
	appendentrReqObject.From_CandidateId = 3

	appendentrReqObject.LeaderLastLog = SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd1")}
	appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:3,Term:2,Data:[]byte("cmd1")}

	ret:=svr.processEvent(appendentrReqObject);

	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	log := (ret[1]).(SERVER_LOG_DATASTR) //assertion
	if 	log.Index == 3 &&
	log.Term == 2{
		//this is expected
	}else {
		t.Error(log)
	}

	appentResp := (ret[2]).(APPEND_ENTRIES_RESPONSE) //assertion
	if appentResp.AppendSuccess == true &&
		appentResp.From_CandidateId == 1 &&
		appentResp.To_CandidateId == 3 &&
		svr.state == FOLLOWER &&
		svr.lastLogIndex == 3 &&
		svr.LOG[svr.lastLogIndex].Term == 2 {
		//this is expected
	}else {
		t.Error("TestAppendEntriesRequesleader1 %d", svr.lastLogIndex)
	}
}

func TestAppendEntriesRequestToLeader2(t *testing.T) { //reject entries from an older term and behaves as leader

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER;
	svr.term = 2;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	//preparing a vote request
	var appendentrReqObject APPEND_ENTRIES_REQUEST
	appendentrReqObject.LeaderTerm = 1
	appendentrReqObject.From_CandidateId = 3

	appendentrReqObject.LeaderLastLog = SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")}
	appendentrReqObject.LogToAdd = SERVER_LOG_DATASTR{Index:2,Term:1,Data:[]byte("cmd1")}

	ret:=svr.processEvent(appendentrReqObject);
	appentResp := (ret[0]).(APPEND_ENTRIES_RESPONSE) //assertion

	if appentResp.AppendSuccess == false &&
		appentResp.From_CandidateId == 1 &&
		appentResp.To_CandidateId == 3 &&
		svr.state == LEADER {
		//this is expected
	}else {
		t.Error("wrong voted at TestAppendEntriescandidate2")
	}

}

//test is sufficient for follower and candidate
func TestTimeoutToFollower(t *testing.T) { //becomes candidate on election timeout, same ofr follower and candidte

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.term = 2;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	ret:=svr.processEvent(TIMEOUT{})

	i:=0

	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error(ret[0])
	}

	for i=1;i<len(svr.candidates)-1;i++ {
		if reflect.TypeOf(ret[i]) != reflect.TypeOf(VOTE_REQUEST{}) {
			t.Error(reflect.TypeOf(ret[i]))
		}
	}
	i++
	timeoutResp := (ret[i]).(RESET_ALARM_ELECTION_TIMEOUT) //assertion
	if 	timeoutResp.duration == svr.election_time_out &&
		svr.state == CANDIDATE {
		//this is expected
	}else {
		t.Error("timeout for election timer got %d", timeoutResp)
	}
}

func TestTimeoutToLeader(t *testing.T) { //sends heartbeat as a leader on timeout

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER;
	svr.term = 2;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	ret:=svr.processEvent(TIMEOUT{})


	i:=0
	for i=0;i<len(svr.candidates)-2;i++ {
		if reflect.TypeOf(ret[i]) != reflect.TypeOf(APPEND_ENTRIES_REQUEST{}) {
			t.Error(reflect.TypeOf(ret[i]))
		}
	}
	i++
	timeoutResp := (ret[i]).(RESET_ALARM_HEARTBEAT_TIMEOUT) //assertion
	if 	timeoutResp.duration == svr.heartbeat_time_out &&
		svr.state == LEADER {
		//this is expected
	}else {
		t.Error("timeout for election timer got %d", timeoutResp)
	}
}

func TestVoteRespToLeader(t *testing.T) { //updates term on a high term message and become follower

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER
	svr.term = 3
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})


	var voteres VOTE_RESPONSE
	voteres.From_CandidateId = 2
	voteres.ResponderTerm = 4
	voteres.Voted = false
	voteres.To_CandidateId = svr.candidateId

	ret:=svr.processEvent(voteres)


	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

//	if reflect.TypeOf(ret[1]) != reflect.TypeOf(NO_ACTION{}){
//		t.Error("unexpected")
//	}

	if svr.term == 4 && svr.state == FOLLOWER {
		//expected
	}else {
		t.Error("term didnt update after receiving a greater term msg")

	}
}

func TestVoteRespToFollower(t *testing.T) { //updates term on a high term message

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER
	svr.term = 3
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	var voteres VOTE_RESPONSE
	voteres.From_CandidateId = 2
	voteres.ResponderTerm = 4
	voteres.Voted = false
	voteres.To_CandidateId = svr.candidateId

	ret:=svr.processEvent(voteres)

	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	if svr.term == 4 && svr.state == FOLLOWER {
		//expected
	}else {
		t.Error("term didnt update after receiving a greater term msg")

	}
//---------------------------------------------------------- ignore a vote response
	svr.state = FOLLOWER

	var voteres2 VOTE_RESPONSE
	voteres2.From_CandidateId = 2
	voteres2.ResponderTerm = 3
	voteres2.Voted = false
	voteres2.To_CandidateId = svr.candidateId

	ret =svr.processEvent(voteres2)
	if reflect.TypeOf(ret[0]) != reflect.TypeOf(NO_ACTION{}){
		t.Error("unexpected")
	}

	if svr.term == 4 && svr.state == FOLLOWER {
		//expected
	}else {
		t.Error("term shloud have remain same")

	}


}

func TestVoteRespToCandidate(t *testing.T) { //updates vote count from valid request and dont update on invalid request

var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = CANDIDATE;
	svr.term = 3;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:3,Data:[]byte("cmd2")})

	var voteres VOTE_RESPONSE
	voteres.From_CandidateId = 2
	voteres.ResponderTerm = 3
	voteres.Voted = true
	voteres.To_CandidateId = svr.candidateId

	svr.processEvent(voteres)

	if svr.candidateStateAttrData.positiveVoteCount == 1 {
		//expected
	}else {
		t.Error("vote didnt increase")

	}

	var voteres2 VOTE_RESPONSE
	voteres2.From_CandidateId = 3
	voteres2.ResponderTerm = 3
	voteres2.Voted = false
	voteres2.To_CandidateId = svr.candidateId

	svr.processEvent(voteres2)

	if 	svr.candidateStateAttrData.positiveVoteCount == 1 &&
		svr.candidateStateAttrData.negativeVoteCount == 1{
		//expected
	}else {
		t.Error("vote shouldnt increase as voted=false")

	}

	var voteres3 VOTE_RESPONSE
	voteres3.From_CandidateId = 4
	voteres3.ResponderTerm = 3
	voteres3.Voted = true
	voteres3.To_CandidateId = svr.candidateId

	svr.processEvent(voteres3)

	if svr.candidateStateAttrData.positiveVoteCount == 2 {
		//expected
	}else {
		t.Error("vote should increase as voted=true")

	}
	var voteres4 VOTE_RESPONSE
	voteres4.From_CandidateId = 4
	voteres4.ResponderTerm = 3
	voteres4.Voted = true
	voteres4.To_CandidateId = svr.candidateId

	ret:=svr.processEvent(voteres4)

//----------------------------------------------become leader as got sufficient votes

	if svr.candidateStateAttrData.positiveVoteCount == 3 {
		//expected
	}else {
		t.Error("vote should increase as voted=true")

	}

	i:=0
	for i=0;i<len(svr.candidates)-2;i++ {
		if reflect.TypeOf(ret[i]) != reflect.TypeOf(APPEND_ENTRIES_REQUEST{}) {
			t.Error(reflect.TypeOf(ret[i]))
		}
	}
	i++
	timeoutResp := (ret[i]).(RESET_ALARM_HEARTBEAT_TIMEOUT) //assertion
	if 	timeoutResp.duration == svr.heartbeat_time_out &&
	svr.state == LEADER {
		//this is expected
	}else {
		t.Error("timeout for election timer got %d", timeoutResp)
	}

}


func TestVoteRespToCandidate2(t *testing.T) { //updates term on a high term message and become follower

var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = CANDIDATE;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	svr.term = 3


	var voteres VOTE_RESPONSE
	voteres.From_CandidateId = 2
	voteres.ResponderTerm = 4
	voteres.Voted = true
	voteres.To_CandidateId = svr.candidateId

	ret :=svr.processEvent(voteres)

	if reflect.TypeOf(ret[0]) != reflect.TypeOf(STATE_STORE{}){
		t.Error("unexpected")
	}

	if svr.state == FOLLOWER {
		//expected
	}else {
		t.Error("should become follower")

	}
}
func TestVoteRespToCandidate3(t *testing.T) { //become follower on majority of negative votes
	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = CANDIDATE;
	svr.term = 3;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:3,Data:[]byte("cmd2")})

	var voteres VOTE_RESPONSE
	voteres.From_CandidateId = 2
	voteres.ResponderTerm = 3
	voteres.Voted = false
	voteres.To_CandidateId = svr.candidateId

	svr.processEvent(voteres)

	if svr.candidateStateAttrData.positiveVoteCount == 0 && svr.candidateStateAttrData.negativeVoteCount ==1 {
		//expected
	}else {
		t.Error("vote shouldnt increase")

	}

	var voteres2 VOTE_RESPONSE
	voteres2.From_CandidateId = 3
	voteres2.ResponderTerm = 3
	voteres2.Voted = false
	voteres2.To_CandidateId = svr.candidateId

	svr.processEvent(voteres2)

	if 	svr.candidateStateAttrData.positiveVoteCount == 0 &&
	svr.candidateStateAttrData.negativeVoteCount == 2{
		//expected
	}else {
		t.Error("vote -ve should increase as voted=false")

	}
	if svr.state == CANDIDATE {
		//this is expected
	}else {
		t.Error("negative vote makes it follower")
	}
	var voteres3 VOTE_RESPONSE
	voteres3.From_CandidateId = 4
	voteres3.ResponderTerm = 3
	voteres3.Voted = false
	voteres3.To_CandidateId = svr.candidateId

	svr.processEvent(voteres3)

	if 	svr.candidateStateAttrData.positiveVoteCount == 0 &&
	svr.candidateStateAttrData.negativeVoteCount == 3{
		//expected
	}else {
		t.Error("-ve vote should increase as voted=false")

	}

	if svr.state == FOLLOWER {
		//this is expected
	}else {
		t.Error("negative vote makes it follower")
	}

}
func TestAppendentriesRespToLeader(t *testing.T) { //on false response decrement nextindex and send again

var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:2,Data:[]byte("cmd2")})
	svr.term = 2

	svr.leaderStateAttrData.nextIndex = make(map[int64]SERVER_LOG_DATASTR)
	svr.leaderStateAttrData.matchIndex = make(map[int64]SERVER_LOG_DATASTR)


	for i := 1; i <= len(svr.candidates); i++ {svr.leaderStateAttrData.matchIndex[int64(i)]=SERVER_LOG_DATASTR{Index:0,Term:0}}
	//suppose candidate two current log is this  [1,1] [2,1] [3,1]

	//this means that server has sent to candidate 2 the nextindexlog that is [3,2](prevIndex:2,2) but candidate 2 has responded with false,
	// hence our server should respons to this case by decreasing nextindex by 1 and sending to candidate:2 again
	var appres APPEND_ENTRIES_RESPONSE
	appres.From_CandidateId = 2

	svr.leaderStateAttrData.nextIndex[appres.From_CandidateId] = svr.LOG[svr.lastLogIndex]
	temp:= svr.LOG[svr.lastLogIndex-1]

	appres.ResponderTerm = 2
	appres.AppendSuccess = false
	appres.To_CandidateId = svr.candidateId

	ret:=svr.processEvent(appres)

	appendReq := (ret[0]).(APPEND_ENTRIES_REQUEST) //assertion
	if 	isEqual(appendReq.LogToAdd ,temp) &&
		isEqual(appendReq.LogToAdd ,svr.leaderStateAttrData.nextIndex[appres.From_CandidateId]){
		//expected
	}else {
		t.Error(appendReq.LogToAdd,temp)

	}

	//removed as part of logic change
//	cmt := (ret[1]).(COMMIT_TO_CLIENT) //assertion
//	if cmt.Index != svr.commitIndex {
//		t.Error(cmt)
//	}

//now the follower is sent a previous log as it said false the last time  [2,2](previndex [1,1])
//so server sendds
	//this would match with follower and follower sends true, now leader should increase nextindex and match index for the
	//candidate : 2
	temp = svr.LOG[svr.lastLogIndex]
	var appres1 APPEND_ENTRIES_RESPONSE

	appres1.From_CandidateId = 2
	appres1.ResponderTerm = 2
	appres1.AppendSuccess = true
	appres1.To_CandidateId = svr.candidateId

	ret = svr.processEvent(appres1)

	appendReq = (ret[0]).(APPEND_ENTRIES_REQUEST) //assertion
	if isEqual(appendReq.LogToAdd ,temp) &&
	isEqual(appendReq.LogToAdd ,svr.leaderStateAttrData.nextIndex[appres.From_CandidateId]){

	}else {

		t.Error(appendReq.LogToAdd,temp)

	}
//	cmt = (ret[1]).(COMMIT_TO_CLIENT) //assertion
//	if cmt.Index != svr.commitIndex {
//		t.Error(cmt)
//	}

}


func TestAppendEntryRespToLeader2(t *testing.T) { //updates term on a high term message becomes follower, on low term keeps doing its work and ignores

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1, Term:1, Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2, Term:2, Data:[]byte("cmd2")})

	svr.term = 2

	var appres1 APPEND_ENTRIES_RESPONSE

	appres1.From_CandidateId = 2
	appres1.ResponderTerm = 3
	appres1.AppendSuccess = true
	appres1.To_CandidateId = svr.candidateId

	svr.processEvent(appres1)

	if svr.state == FOLLOWER {
		//expected
	}else {
		t.Error("should become follower")

	}

}

func TestAppendEntryRespToLeader3(t *testing.T) { //low term of the message , leader keeps doing its work and ignores

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId
	svr.term = 3

	var appres1 APPEND_ENTRIES_RESPONSE

	appres1.From_CandidateId = 2
	appres1.ResponderTerm = 2
	appres1.AppendSuccess = true
	appres1.To_CandidateId = svr.candidateId

	svr.processEvent(appres1)

	if svr.state == LEADER {
		//expected
	}else {
		t.Error("should stay leader")

	}

}


func TestAppendEntryRespToCandidateNFollower1_2_3(t *testing.T) { //updates term on a high term message becomes follower, on low term keeps doing its work and ignores

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = CANDIDATE;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})

	svr.term = 2

	var appres1 APPEND_ENTRIES_RESPONSE

	appres1.From_CandidateId = 2
	appres1.ResponderTerm = 3
	appres1.AppendSuccess = true
	appres1.To_CandidateId = svr.candidateId

	svr.processEvent(appres1)

	if svr.state == FOLLOWER {
		//expected
	}else {
		t.Error("should become follower")

	}
//------------------ignores lower term msg------------------
	svr.state = FOLLOWER
	svr.term = 3


	appres1.From_CandidateId = 2
	appres1.ResponderTerm = 2
	appres1.AppendSuccess = true
	appres1.To_CandidateId = svr.candidateId

	svr.processEvent(appres1)

	if svr.state == FOLLOWER {
		//expected
	}else {
		t.Error("should stay follower")

	}


}
func TestAppendEntryToLeader(t *testing.T) { //leader appends to own log and send out to others

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1, Term:1, Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2, Term:2, Data:[]byte("cmd2")})

	svr.term = 2

	ret:= svr.processEvent([]byte("abcd"))
	i:=0
	log := (ret[i]).(SERVER_LOG_DATASTR) //assertion
	if 	log.Index == 3 &&
		log.Term ==2{
		//this is expecte
	}else {
		t.Error(log)
	}
	i++;
	for ;i<len(svr.candidates)-2;i++ {
		if reflect.TypeOf(ret[i]) != reflect.TypeOf(APPEND_ENTRIES_REQUEST{}) {
			t.Error(reflect.TypeOf(ret[i]))
		}
	}
	//i=i
}

func TestAppendEntryToFollowerNCandidate(t *testing.T) { //redirects append req to leader

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = FOLLOWER;
	svr.lastLogIndex = 1
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1, Term:1, Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2, Term:2, Data:[]byte("cmd2")})

	svr.term = 2

	ret:= svr.processEvent([]byte("abcd"))
	i:=0
	adata := (ret[i]).(COMMIT_TO_CLIENT) //assertion
	if 	adata.Err_code == ERR_NOT_LEADER{
		//this is expected
	}else {
		t.Error(adata)
	}

}

func TestCommitIndex(t *testing.T) { //check resulting commit index on different scenarios

	var svr SERVER_DATA
	svr.setBasicMachine()

	//TEST STATE SETUP

	svr.state = LEADER;
	svr.lastLogIndex = 7
	svr.votedFor = svr.candidateId

	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:1,Term:1,Data:[]byte("cmd1")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:2,Term:2,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:3,Term:2,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:4,Term:2,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:5,Term:2,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:6,Term:3,Data:[]byte("cmd2")})
	svr.addThisLogEntry(SERVER_LOG_DATASTR{Index:7,Term:3,Data:[]byte("cmd2")})

	//above is current server log

	svr.term = 3
	svr.commitIndex = 2
	svr.leaderStateAttrData.matchIndex = make(map[int64]SERVER_LOG_DATASTR)

	//match index of other servers


	svr.leaderStateAttrData.matchIndex[2] = SERVER_LOG_DATASTR{Index:3,Term:3,Data:[]byte("cmd2")}
	svr.leaderStateAttrData.matchIndex[3] = SERVER_LOG_DATASTR{Index:3,Term:3,Data:[]byte("cmd2")}
	svr.leaderStateAttrData.matchIndex[4] = SERVER_LOG_DATASTR{Index:3,Term:3,Data:[]byte("cmd2")}
	svr.leaderStateAttrData.matchIndex[5] = SERVER_LOG_DATASTR{Index:6,Term:3,Data:[]byte("cmd2")}



	//actions := make([]interface{}, 0)
	ret:=svr.commitCheck()

	cmt := (ret).(COMMIT_TO_CLIENT) //assertion

	if cmt.Index == 3 && svr.commitIndex == 3 {
		//expected
	}else{
		t.Error(cmt)
	}

//-------------------------------------------------------------

	svr.leaderStateAttrData.matchIndex[2] = SERVER_LOG_DATASTR{Index:3,Term:2,Data:[]byte("cmd2")}
	svr.leaderStateAttrData.matchIndex[3] = SERVER_LOG_DATASTR{Index:3,Term:2,Data:[]byte("cmd2")}
	svr.leaderStateAttrData.matchIndex[4] = SERVER_LOG_DATASTR{Index:6,Term:3,Data:[]byte("cmd2")}
	svr.leaderStateAttrData.matchIndex[5] = SERVER_LOG_DATASTR{Index:6,Term:3,Data:[]byte("cmd2")}

	//actions := make([]interface{}, 0)
	ret = svr.commitCheck()
	cmt = (ret).(COMMIT_TO_CLIENT) //assertion
	if cmt.Index !=6 {
		t.Error(cmt)
	}




}
