package main

import (
	"math"
	"reflect"
)

//func main() {
//	//beginStateMachine()
//
//}

func (thisServer *SERVER_DATA) processEvent(incoming interface{})(actions[]interface{}) {

	//for sending an invalid command response
	noactions := make([]interface{},0)
	noactions = append(noactions,NO_ACTION{})

	switch{
	case reflect.TypeOf(incoming) == reflect.TypeOf(VOTE_REQUEST{}):
		return thisServer.voteRequest(incoming.(VOTE_REQUEST))

	case reflect.TypeOf(incoming) == reflect.TypeOf(VOTE_RESPONSE{}):
		return thisServer.voteResponse(incoming.(VOTE_RESPONSE))

	case reflect.TypeOf(incoming) == reflect.TypeOf(APPEND_ENTRIES_REQUEST{}):
		return thisServer.appendEntriesRequest(incoming.(APPEND_ENTRIES_REQUEST))

	case reflect.TypeOf(incoming) == reflect.TypeOf(APPEND_ENTRIES_RESPONSE{}):
		return thisServer.appendEntriesResponse(incoming.(APPEND_ENTRIES_RESPONSE))

	case reflect.TypeOf(incoming) == reflect.TypeOf(TIMEOUT{}):
		return thisServer.timeoutRequest()

	case reflect.TypeOf(incoming).String() == APPEND_DATA:
		return thisServer.append(incoming.([]uint8))

	default:
		return noactions
	}
}

func (thisServer *SERVER_DATA) voteRequest(incomingReq VOTE_REQUEST)(actions[]interface{}) {

	switch{

	case thisServer.state == FOLLOWER :
		actions = thisServer.followerVoteRequest(incomingReq)

	case thisServer.state == CANDIDATE || thisServer.state == LEADER :
		actions = thisServer.candidateVoteRequest(incomingReq)

	}
	return  actions
}

func (thisServer *SERVER_DATA) appendEntriesRequest(incomingReq APPEND_ENTRIES_REQUEST)(actions []interface{}) {

	actions = thisServer.followerLeaderCandidateAppendEntriesRequest(incomingReq)
return actions 
}

func (thisServer *SERVER_DATA) timeoutRequest()(actions []interface{}) {

	switch{

	case thisServer.state == FOLLOWER || thisServer.state == CANDIDATE :
		actions = thisServer.followerTimeoutRequest()

	case thisServer.state == LEADER :
		actions = thisServer.leaderTimeoutRequest()
	}
	return actions 
}

func (thisServer *SERVER_DATA) voteResponse(incomingRes VOTE_RESPONSE)(actions []interface{}) {

	switch{

	case thisServer.state == FOLLOWER :
		actions = thisServer.followerVoteResponse(incomingRes)
	case thisServer.state == CANDIDATE :
		actions = thisServer.candidateVoteResponse(incomingRes)
	case thisServer.state == LEADER :
		actions = thisServer.leaderVoteResponse(incomingRes)
	}
return actions
}
func (thisServer *SERVER_DATA) appendEntriesResponse(incomingRes APPEND_ENTRIES_RESPONSE)(actions []interface{}) {

	switch{

	case thisServer.state == FOLLOWER || thisServer.state == CANDIDATE :
		actions = thisServer.followerAndCandidateAppendEntriesResponse(incomingRes)
	case thisServer.state == LEADER :
		actions = thisServer.leaderAppendEntriesResponse(incomingRes)
	}
	return actions
}


func (thisServer *SERVER_DATA) followerVoteRequest(incomingReq VOTE_REQUEST)(actions []interface{}) {
	var voteResObj VOTE_RESPONSE //vote response object
	actions = make([]interface{},0)


	if thisServer.onSameTermWith(incomingReq) && thisServer.
	hasNotVoted() && thisServer.LogIsNotMoreUpdatedThan(incomingReq) { //accept path

		thisServer.term = incomingReq.electionTerm
		thisServer.votedFor = incomingReq.from_CandidateId

		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})

		voteResObj.makeResp(thisServer, incomingReq, true)
		actions = append(actions,voteResObj)


	}else if (thisServer.term < incomingReq.electionTerm) { //candidate on higher term



		thisServer.term = incomingReq.electionTerm
		if thisServer.LogIsNotMoreUpdatedThan(incomingReq){ //is he sufficiently updated

			thisServer.votedFor = incomingReq.from_CandidateId
			voteResObj.makeResp(thisServer, incomingReq, true)
		}else{
			thisServer.votedFor =NONE
			voteResObj.makeResp(thisServer, incomingReq, false)
		}

		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})
		actions = append(actions,voteResObj)

	}else {

		voteResObj.makeResp(thisServer, incomingReq, false)
		actions = append(actions,voteResObj)

	}
return actions
}
func (thisServer *SERVER_DATA) candidateVoteRequest(incomingReq VOTE_REQUEST)(actions []interface{}) {
	var voteResObj VOTE_RESPONSE //vote response object
	actions = make([]interface{}, 0)


	if (thisServer.term < incomingReq.electionTerm) { //give vote to >= term

		thisServer.term = incomingReq.electionTerm
		thisServer.votedFor = incomingReq.from_CandidateId
		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})

		thisServer.state = FOLLOWER
		voteResObj.makeResp(thisServer, incomingReq, true)
		actions = append(actions,voteResObj)

	}else { //dont give vote to lower term

		voteResObj.makeResp(thisServer, incomingReq, false)
		actions = append(actions,voteResObj)
	}


	actions = append(actions,voteResObj)

	return actions

}

func (thisServer *SERVER_DATA) followerLeaderCandidateAppendEntriesRequest(incomingReq APPEND_ENTRIES_REQUEST)(actions []interface{}) {
	var appentResObj APPEND_ENTRIES_RESPONSE //vote response object

	actions = make([]interface{}, 0)


	if (thisServer.term > incomingReq.leaderTerm) {

		//if in candidate state then keep behaving as candidate
		appentResObj.makeResp(thisServer, incomingReq, false)
		actions = append(actions,appentResObj)

	}else {

		if thisServer.term <incomingReq.leaderTerm{

			//reset vote as soon as a term update
			thisServer.votedFor = NONE
			thisServer.term = incomingReq.leaderTerm

			actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})

		}
		if thisServer.state == CANDIDATE || thisServer.state == LEADER {
			thisServer.state = FOLLOWER
		}


		if (incomingReq.logToAdd.isHeartbeat == true) {  //this is a heartbeat message

			//fmt.Print("here22=");fmt.Print(incomingReq.logToAdd)
			actions = append(actions,RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})

		}else if thisServer.SomeLastLogIsSameAs(incomingReq) { //after (decreasing) next index log have matched now

			thisServer.lastLogIndex = incomingReq.leaderLastLog.index
			thisServer.addThisEntry(incomingReq.logToAdd)
			actions = append(actions,incomingReq.logToAdd)

			appentResObj.makeResp(thisServer, incomingReq, true)

			thisServer.commitIndex = int64(math.Min(float64(incomingReq.leaderCommitIndex),
				float64(thisServer.lastLogIndex)))
			actions = append(actions,appentResObj)
			actions = append(actions,RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})


		}else {

			appentResObj.makeResp(thisServer, incomingReq, false)//leader on receiving this  will decrement nextindex
			actions = append(actions,appentResObj)
			actions = append(actions,RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})//test for this

		}
	}
	return actions
}

func (thisServer *SERVER_DATA) followerTimeoutRequest()(actions []interface{}) {
	actions = make([]interface{},0)


	thisServer.state = CANDIDATE //changed from FOLLOWER to CANDIDATE


	thisServer.term = thisServer.term + 1 //increase term
	thisServer.votedFor = thisServer.candidateId //vote to self

	actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})


	thisServer.candidateStateAttrData.negativeVoteCount = 0; //set-up/reset a counter for vote for this term
	thisServer.candidateStateAttrData.positiveVoteCount = 0; //set-up/reset a counter for vote for this term

	//request votevoteR

	//var voteReq VOTE_REQUEST
	for i := 0; i < len(thisServer.candidates); i++ {

		if thisServer.candidates[i] == thisServer.candidateId {
			continue
		}
		voteReq := makeReq(thisServer, thisServer.candidates[i])
		actions = append(actions,voteReq)
	}

	thisServer.randomizeTimeout()
	actions = append(actions,RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})
	 //raise timeout event again if no winner in the election interval
	return actions
}


func (thisServer *SERVER_DATA) leaderTimeoutRequest()(actions []interface{}){
	actions = make([]interface{}, 0)

	for i := 0; i < len(thisServer.candidates); i++ {

		if thisServer.candidates[i] == thisServer.candidateId {
			continue
		}
		//preparing heartbeat request
		appendReq := makeAppendEntryReq(thisServer, thisServer.candidates[i], true)
		actions = append(actions,appendReq)
	}

	actions = append(actions,RESET_ALARM_HEARTBEAT_TIMEOUT{duration:thisServer.heartbeat_time_out})
	//raise timeout event again if no winner in the election interval
	return actions
}

func (thisServer *SERVER_DATA) followerVoteResponse(incomingRes VOTE_RESPONSE)(actions []interface{}) {
	actions = make([]interface{}, 0)
	if (thisServer.term < incomingRes.responderTerm) {


		thisServer.term = incomingRes.responderTerm
		thisServer.votedFor = NONE

		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})

	}else {
		// ignore case
	}
	actions = append(actions,NO_ACTION{})

return actions
}
func (thisServer *SERVER_DATA) candidateVoteResponse(incomingRes VOTE_RESPONSE)(actions []interface{}) {
	actions = make([]interface{}, 0)

	if incomingRes.responderTerm > thisServer.term {


		thisServer.state = FOLLOWER
		thisServer.term = incomingRes.responderTerm
		thisServer.votedFor = NONE

		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})

		//actions = append(actions,NO_ACTION{})


	}else  {

		if incomingRes.voted == true {
			thisServer.candidateStateAttrData.positiveVoteCount ++

			if thisServer.candidateStateAttrData.positiveVoteCount >= int64(majorityCount(thisServer.candidates)) {

				thisServer.state = LEADER //changed from CANDIDATE to LEADER

				//setting value for leader attributes for all servers
				for i := 0; i < len(thisServer.candidates); i++ {
					thisServer.leaderStateAttrData.nextIndex = make(map[int64]SERVER_LOG_DATASTR)
					thisServer.leaderStateAttrData.matchIndex = make(map[int64]SERVER_LOG_DATASTR)

					thisServer.leaderStateAttrData.nextIndex[thisServer.candidates[i]] =
										thisServer.LOG[thisServer.lastLogIndex + 1] //for all servers
					var alog SERVER_LOG_DATASTR
					alog.makeLog(0, 0, []byte("")) //zerolog as intialization to matchindex
					thisServer.leaderStateAttrData.matchIndex[thisServer.candidates[i]] = alog //for all servers
				}

				actions = thisServer.leaderTimeoutRequest()

			}
		}else { //voted false

			thisServer.candidateStateAttrData.negativeVoteCount ++

			if thisServer.candidateStateAttrData.negativeVoteCount >= int64(majorityCount(thisServer.candidates)) {
				thisServer.state = FOLLOWER
			}

		}
	}
	return actions
}
func (thisServer *SERVER_DATA) leaderVoteResponse(incomingRes VOTE_RESPONSE)(actions []interface{}) {
	actions = make([]interface{}, 0)


	if (thisServer.term < incomingRes.responderTerm) {


		thisServer.state = FOLLOWER
		thisServer.term = incomingRes.responderTerm
		thisServer.votedFor = NONE
		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})


	}
	actions = append(actions,NO_ACTION{})
	return actions
}

func (thisServer *SERVER_DATA) followerAndCandidateAppendEntriesResponse(incomingRes APPEND_ENTRIES_RESPONSE)(actions []interface{}) {
	actions = make([]interface{}, 0)

	if (thisServer.term < incomingRes.responderTerm) {


		thisServer.term = incomingRes.responderTerm
		thisServer.state = FOLLOWER
		thisServer.votedFor = NONE
		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})


	}else {
		// ignore case
	}

	actions = append(actions,NO_ACTION{})
	return actions
}

func (thisServer *SERVER_DATA) leaderAppendEntriesResponse(incomingRes APPEND_ENTRIES_RESPONSE)(actions []interface{}) {

	var actionElem interface{}
	//fmt.Print(incomingRes)

	actions = make([]interface{}, 0)

	if (thisServer.term < incomingRes.responderTerm) {


		thisServer.term = incomingRes.responderTerm
		thisServer.state = FOLLOWER
		thisServer.votedFor = NONE
		actions = append(actions,STATE_STORE{term:thisServer.term,voteFor:thisServer.votedFor})

		actions = append(actions,NO_ACTION{})


	}else if thisServer.term > incomingRes.responderTerm {
		//ignore

		actions = append(actions,NO_ACTION{})

	}else {  //if equal then

		actionElem = thisServer.commitCheck()


		if incomingRes.appendSuccess == false {

			nexttobeindex:= thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId].index - 1
			thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId] = thisServer.LOG[nexttobeindex]

			//send a decreased next index log for match and keep doing that until tru response is obtained
			var appendentrReqObject APPEND_ENTRIES_REQUEST
			appendentrReqObject.leaderTerm = thisServer.term
			appendentrReqObject.from_CandidateId = thisServer.candidateId
			appendentrReqObject.leaderCommitIndex = thisServer.commitIndex
			appendentrReqObject.to_CandidateId = incomingRes.from_CandidateId
			appendentrReqObject.leaderLastLog = thisServer.LOG[nexttobeindex - 1]
			appendentrReqObject.logToAdd = thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId]

			actions = append(actions,appendentrReqObject)

		}else { //response is true


			//we are sure that the server has succesfully replicated the log that was sent to
			// it(which is marked by nextIndex)
			thisServer.leaderStateAttrData.matchIndex[incomingRes.from_CandidateId] =
			thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId];


			//until follower has identical log keep sending next index
			if thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId].index < (thisServer.lastLogIndex+1){

				nexttobeindex:= thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId].index + 1
				thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId] = thisServer.LOG[nexttobeindex]


				var appendentrReqObject APPEND_ENTRIES_REQUEST
				appendentrReqObject.leaderTerm = thisServer.term
				appendentrReqObject.from_CandidateId = thisServer.candidateId
				appendentrReqObject.leaderCommitIndex = thisServer.commitIndex
				appendentrReqObject.to_CandidateId = incomingRes.from_CandidateId
				appendentrReqObject.leaderLastLog = thisServer.LOG[nexttobeindex]
				appendentrReqObject.logToAdd = thisServer.leaderStateAttrData.nextIndex[incomingRes.from_CandidateId]

				actions = append(actions,appendentrReqObject)



			}

		}

	}
	actions = append(actions,actionElem)

	return actions
}

func (thisServer *SERVER_DATA) commitCheck()(actionElem interface{}){

	counter := 0
	//intializing with current commit index , but if any latest thing commits this will change at lineNO 501
	actionElem = COMMIT_TO_CLIENT{index:thisServer.commitIndex,
		data:thisServer.LOG[thisServer.commitIndex].data, err_code:0}
	//
	indexUnderCheck := thisServer.lastLogIndex//will check from leader lastlog to leader commited log, in the next for loop

	for {
		counter = 0

		for i := 1; i <= len(thisServer.candidates); i++ {

			if thisServer.candidateId == int64(i) {continue}

			if 	thisServer.leaderStateAttrData.matchIndex[int64(i)].index >= (indexUnderCheck) &&
				thisServer.leaderStateAttrData.matchIndex[int64(i)].term == thisServer.term {
				counter++;//fmt.Println(i)
			}
		}
		if	thisServer.LOG[thisServer.lastLogIndex].index >= indexUnderCheck &&
			thisServer.LOG[thisServer.lastLogIndex].term == thisServer.term {

			counter++;
		}
		//if majority has matched index then update commit index
		if 	(counter) >= int(majorityCount(thisServer.candidates)) {
			thisServer.commitIndex = indexUnderCheck; //send commit to client here
			//fmt.Println(indexUnderCheck)
			actionElem = COMMIT_TO_CLIENT{index:thisServer.commitIndex,
				data:thisServer.LOG[thisServer.commitIndex].data, err_code:0}
			break;

		}else {

			if indexUnderCheck > thisServer.commitIndex {
				indexUnderCheck --;
			}else {

				break
			}
		}

	}

return  actionElem
}
//client calls this append for request
func (thisServer *SERVER_DATA) append(data []byte)(actions []interface{}) {

	var actionElem interface{}
	actions = make([]interface{}, 0)

	if thisServer.state == CANDIDATE || thisServer.state == FOLLOWER{

		//redirect to last known leader
		actions = append(actions,REDIRECT_APPEND_DATA{data:data,redirectToiId:thisServer.leaderId})

	}else if thisServer.state == LEADER{

		//add to own log first , its ad to the in-memory map for now
		thisServer.addThisEntry(SERVER_LOG_DATASTR{index:thisServer.lastLogIndex,term:thisServer.term,data:data})
		//here the logstore operation action is added
		actions = append(actions,SERVER_LOG_DATASTR{index:thisServer.lastLogIndex,term:thisServer.term,data:data})

		actionElem = thisServer.commitCheck()


	//prepare to send to others
		for i := 0; i < len(thisServer.candidates); i++ {

			if thisServer.candidates[i]==thisServer.candidateId{
				continue
			}
			var appendentrReqObject APPEND_ENTRIES_REQUEST
			appendentrReqObject.leaderTerm = thisServer.term
			appendentrReqObject.from_CandidateId = thisServer.candidateId
			appendentrReqObject.leaderCommitIndex = thisServer.commitIndex
			appendentrReqObject.to_CandidateId = (thisServer.candidates[i])
			nextIndex:= thisServer.leaderStateAttrData.nextIndex[int64(i)].index
			appendentrReqObject.leaderLastLog = thisServer.LOG[nextIndex-1]
			appendentrReqObject.logToAdd = thisServer.LOG[nextIndex]

			actions = append(actions,appendentrReqObject)

		}


	}

	actions = append(actions,actionElem)
	return actions
}
