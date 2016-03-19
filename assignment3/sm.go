package main

import (
	"math"
	"reflect"
	"fmt"
)

//func main() {
//	//beginStateMachine()
//
//}

func (thisServer *SERVER_DATA) processEvent(incoming interface{}) (actions[]interface{}) {

	//for sending an invalid command response
	noactions := make([]interface{}, 0)
	noactions = append(noactions, NO_ACTION{})

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

func (thisServer *SERVER_DATA) voteRequest(incomingReq VOTE_REQUEST) (actions[]interface{}) {

	switch{

	case thisServer.state == FOLLOWER :
		actions = thisServer.followerVoteRequest(incomingReq)

	case thisServer.state == CANDIDATE || thisServer.state == LEADER :
		actions = thisServer.candidateVoteRequest(incomingReq)

	}
	return actions
}

func (thisServer *SERVER_DATA) appendEntriesRequest(incomingReq APPEND_ENTRIES_REQUEST) (actions []interface{}) {

	actions = thisServer.followerLeaderCandidateAppendEntriesRequest(incomingReq)
	return actions
}

func (thisServer *SERVER_DATA) timeoutRequest() (actions []interface{}) {

	//debug_output("timeout at statemachine")

	switch{

	case thisServer.state == FOLLOWER || thisServer.state == CANDIDATE :
		actions = thisServer.followerTimeoutRequest()

	case thisServer.state == LEADER :
		actions = thisServer.leaderTimeoutRequest()
	}

	return actions
}

func (thisServer *SERVER_DATA) voteResponse(incomingRes VOTE_RESPONSE) (actions []interface{}) {

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
func (thisServer *SERVER_DATA) appendEntriesResponse(incomingRes APPEND_ENTRIES_RESPONSE) (actions []interface{}) {

	switch{

	case thisServer.state == FOLLOWER || thisServer.state == CANDIDATE :
		actions = thisServer.followerAndCandidateAppendEntriesResponse(incomingRes)
	case thisServer.state == LEADER :
		actions = thisServer.leaderAppendEntriesResponse(incomingRes)
	}
	return actions
}


func (thisServer *SERVER_DATA) followerVoteRequest(incomingReq VOTE_REQUEST) (actions []interface{}) {
	var voteResObj VOTE_RESPONSE //vote response object
	actions = make([]interface{}, 0)


	if thisServer.onSameTermWith(incomingReq) && thisServer.
	hasNotVoted() && thisServer.LogIsNotMoreUpdatedThan(incomingReq) { //accept path

		thisServer.term = incomingReq.ElectionTerm
		thisServer.votedFor = incomingReq.From_CandidateId

		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})

		voteResObj.makeResp(thisServer, incomingReq, true)
		actions = append(actions, voteResObj)


	}else if (thisServer.term < incomingReq.ElectionTerm) { //candidate on higher term


		thisServer.term = incomingReq.ElectionTerm
		if thisServer.LogIsNotMoreUpdatedThan(incomingReq) { //is he sufficiently updated

			thisServer.votedFor = incomingReq.From_CandidateId
			voteResObj.makeResp(thisServer, incomingReq, true)
		}else {
			thisServer.votedFor = NONE
			voteResObj.makeResp(thisServer, incomingReq, false)
		}

		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})
		actions = append(actions, voteResObj)

	}else {

		voteResObj.makeResp(thisServer, incomingReq, false)
		actions = append(actions, voteResObj)

	}
	return actions
}
func (thisServer *SERVER_DATA) candidateVoteRequest(incomingReq VOTE_REQUEST) (actions []interface{}) {
	var voteResObj VOTE_RESPONSE //vote response object
	actions = make([]interface{}, 0)

	if (thisServer.term < incomingReq.ElectionTerm) { //give vote to >= term

		thisServer.term = incomingReq.ElectionTerm
		thisServer.votedFor = NONE
		if thisServer.LogIsNotMoreUpdatedThan(incomingReq) { //is he sufficiently updated

			thisServer.votedFor = incomingReq.From_CandidateId
			thisServer.state = FOLLOWER
			voteResObj.makeResp(thisServer, incomingReq, true)
		}else {
			thisServer.votedFor = NONE
			voteResObj.makeResp(thisServer, incomingReq, false)
		}
		//thisServer.votedFor = incomingReq.From_CandidateId
		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})


		//voteResObj.makeResp(thisServer, incomingReq, true)
		actions = append(actions, voteResObj)

	}else { //dont give vote to lower term

		voteResObj.makeResp(thisServer, incomingReq, false)
		actions = append(actions, voteResObj)
	}


	return actions

}

func (thisServer *SERVER_DATA) followerLeaderCandidateAppendEntriesRequest(incomingReq APPEND_ENTRIES_REQUEST) (actions []interface{}) {
	var appentResObj APPEND_ENTRIES_RESPONSE //vote response object

	actions = make([]interface{}, 0)



	if (thisServer.term > incomingReq.LeaderTerm) {

		//if in candidate state then keep behaving as candidate
		appentResObj.makeResp(thisServer, incomingReq, false)
		actions = append(actions, appentResObj)

	}else {

		if thisServer.term < incomingReq.LeaderTerm {

			//reset vote as soon as a term update
			thisServer.votedFor = NONE
			thisServer.term = incomingReq.LeaderTerm

			actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})

		}
		if thisServer.state == CANDIDATE || thisServer.state == LEADER {
			thisServer.state = FOLLOWER
		}


		if (incomingReq.IsHeartbeat == true) {  //this is a heartbeat message

			//fmt.Print("here22=");fmt.Print(incomingReq.LogToAdd)
			thisServer.leaderId = incomingReq.From_CandidateId
			temp := thisServer.commitIndex


			thisServer.commitIndex = int64(math.Min(float64(incomingReq.LeaderCommitIndex),
				float64(thisServer.lastLogIndex)))
			if thisServer.commitIndex > temp {
				actions = append(actions, COMMIT_TO_CLIENT{Index:thisServer.commitIndex, Data:thisServer.LOG[thisServer.commitIndex].Data})
			}


			actions = append(actions, RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})


		}else if thisServer.SomeLastLogIsSameAs(incomingReq) { //after (decreasing) next index log have matched now


			thisServer.lastLogIndex = incomingReq.LeaderLastLog.Index
			thisServer.addThisEntry(incomingReq.LogToAdd)
			actions = append(actions, incomingReq.LogToAdd)

			appentResObj.makeResp(thisServer, incomingReq, true)

			//			thisServer.commitIndex = int64(math.Min(float64(incomingReq.LeaderCommitIndex),
			//				float64(thisServer.lastLogIndex)))
			actions = append(actions, appentResObj)
			actions = append(actions, RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})


		}else {

			appentResObj.makeResp(thisServer, incomingReq, false)//leader on receiving this  will decrement nextindex
			actions = append(actions, appentResObj)
			actions = append(actions, RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})//test for this

		}
	}
	thisServer.debug_output2("appendactions:::", actions)
	return actions
}
func (thisServer *SERVER_DATA) debug_output2(s string, i interface{}) {

	return

	fmt.Print("**********NOde Id:")
	fmt.Println(thisServer.candidateId)
	fmt.Print(s)
	fmt.Print(i)
	fmt.Println()
	fmt.Println("********end********")

}
func (thisServer *SERVER_DATA) followerTimeoutRequest() (actions []interface{}) {
	actions = make([]interface{}, 0)


	thisServer.state = CANDIDATE //changed from FOLLOWER to CANDIDATE
	//debug_output(thisServer)

	thisServer.term = thisServer.term + 1 //increase term
	thisServer.votedFor = thisServer.candidateId //vote to self
	thisServer.candidateStateAttrData.negativeVoteCount = 0; //set-up/reset a counter for vote for this term
	thisServer.candidateStateAttrData.positiveVoteCount = 0; //set-up/reset a counter for vote for this term

	//debug_output2(" became candidate node :.", thisServer.candidateId)
	thisServer.candidateStateAttrData.positiveVoteCount++;
	//debug_output2("vote count:", thisServer.candidateStateAttrData.positiveVoteCount);
	//debug_output2("term:", thisServer.term)
	actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})




	//request votevoteR

	//var voteReq VOTE_REQUEST
	for i := 0; i < len(thisServer.candidates); i++ {

		if thisServer.candidates[i] == thisServer.candidateId {
			continue
		}
		voteReq := makeReq(thisServer, thisServer.candidates[i])
		actions = append(actions, voteReq)
	}

	//thisServer.randomizeTimeout()
	actions = append(actions, RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})
	//debug_output2("actions dump", RESET_ALARM_ELECTION_TIMEOUT{duration:thisServer.election_time_out})
	//raise timeout event again if no winner in the election interval
	return actions
}


func (thisServer *SERVER_DATA) leaderTimeoutRequest() (actions []interface{}) {
	actions = make([]interface{}, 0)

	for i := 0; i < len(thisServer.candidates); i++ {

		if thisServer.candidates[i] == thisServer.candidateId {
			continue
		}
		//preparing heartbeat request
		appendReq := makeAppendEntryReq(thisServer, thisServer.candidates[i], true)
		actions = append(actions, appendReq)
	}

	actions = append(actions, RESET_ALARM_HEARTBEAT_TIMEOUT{duration:thisServer.heartbeat_time_out})
	//raise timeout event again if no winner in the election interval

	actionElem := thisServer.commitCheck()
	if actionElem != nil {
		actions = append(actions, actionElem)
	}
	return actions
}

func (thisServer *SERVER_DATA) followerVoteResponse(incomingRes VOTE_RESPONSE) (actions []interface{}) {
	actions = make([]interface{}, 0)
	if (thisServer.term < incomingRes.ResponderTerm) {


		thisServer.term = incomingRes.ResponderTerm
		thisServer.votedFor = NONE

		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})

	}else {
		// ignore case
	}
	actions = append(actions, NO_ACTION{})

	return actions
}
func (thisServer *SERVER_DATA) candidateVoteResponse(incomingRes VOTE_RESPONSE) (actions []interface{}) {
	actions = make([]interface{}, 0)

	if incomingRes.ResponderTerm > thisServer.term {


		thisServer.state = FOLLOWER
		thisServer.term = incomingRes.ResponderTerm
		thisServer.votedFor = NONE

		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})

		//actions = append(actions,NO_ACTION{})


	}else {

		if incomingRes.Voted == true {
			thisServer.debug_output2("vote givenBY", incomingRes.From_CandidateId)
			thisServer.candidateStateAttrData.positiveVoteCount ++
			thisServer.debug_output2("vote count", thisServer.candidateStateAttrData.positiveVoteCount)

			if thisServer.candidateStateAttrData.positiveVoteCount >= int64(majorityCount(thisServer.candidates)) {

				thisServer.state = LEADER //changed from CANDIDATE to LEADER
//				fmt.Print("Leader change:")
//				fmt.Print(thisServer.candidateId)
//				fmt.Println(time.Now())

				thisServer.leaderId = thisServer.candidateId
				//fmt.Print("Leader is BOrn:");fmt.Println(thisServer.candidateId)

				//setting value for leader attributes for all servers

				thisServer.leaderStateAttrData.nextIndex = make(map[int64]SERVER_LOG_DATASTR)
				thisServer.leaderStateAttrData.matchIndex = make(map[int64]SERVER_LOG_DATASTR)

				for i := 0; i < len(thisServer.candidates); i++ {


					thisServer.leaderStateAttrData.nextIndex[thisServer.candidates[i]] = SERVER_LOG_DATASTR{Index:thisServer.lastLogIndex + 1}
					//thisServer.LOG[thisServer.lastLogIndex + 1] //for all servers
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
	thisServer.debug_output2("+vote", thisServer.candidateStateAttrData.positiveVoteCount)
	return actions
}
func (thisServer *SERVER_DATA) leaderVoteResponse(incomingRes VOTE_RESPONSE) (actions []interface{}) {
	actions = make([]interface{}, 0)


	if (thisServer.term < incomingRes.ResponderTerm) {


		thisServer.state = FOLLOWER
		thisServer.term = incomingRes.ResponderTerm
		thisServer.votedFor = NONE
		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})


	}
	actions = append(actions, NO_ACTION{})
	return actions
}

func (thisServer *SERVER_DATA) followerAndCandidateAppendEntriesResponse(incomingRes APPEND_ENTRIES_RESPONSE) (actions []interface{}) {
	actions = make([]interface{}, 0)

	if (thisServer.term < incomingRes.ResponderTerm) {


		thisServer.term = incomingRes.ResponderTerm
		thisServer.state = FOLLOWER
		thisServer.votedFor = NONE
		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})


	}else {
		// ignore case
	}

	actions = append(actions, NO_ACTION{})
	return actions
}

func (thisServer *SERVER_DATA) leaderAppendEntriesResponse(incomingRes APPEND_ENTRIES_RESPONSE) (actions []interface{}) {

	//	var actionElem interface{}
	//fmt.Print(incomingRes)

	actions = make([]interface{}, 0)

	if (thisServer.term < incomingRes.ResponderTerm) {


		thisServer.term = incomingRes.ResponderTerm
		thisServer.state = FOLLOWER
		thisServer.votedFor = NONE
		actions = append(actions, STATE_STORE{term:thisServer.term, voteFor:thisServer.votedFor})

		actions = append(actions, NO_ACTION{})


	}else if thisServer.term > incomingRes.ResponderTerm {
		//ignore

		actions = append(actions, NO_ACTION{})

	}else {  //if equal then

		//actionElem = thisServer.commitCheck()


		if incomingRes.AppendSuccess == false {

			nexttobeindex := thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId].Index - 1
			thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId] = thisServer.LOG[nexttobeindex]

			//send a decreased next index log for match and keep doing that until tru response is obtained
			var appendentrReqObject APPEND_ENTRIES_REQUEST
			appendentrReqObject.LeaderTerm = thisServer.term
			appendentrReqObject.From_CandidateId = thisServer.candidateId
			appendentrReqObject.LeaderCommitIndex = thisServer.commitIndex
			appendentrReqObject.To_CandidateId = incomingRes.From_CandidateId
			appendentrReqObject.LeaderLastLog = thisServer.LOG[nexttobeindex - 1]
			appendentrReqObject.LogToAdd = thisServer.LOG[thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId].Index]

			actions = append(actions, appendentrReqObject)

		}else { //response is true


			//we are sure that the server has succesfully replicated the log that was sent to
			// it(which is marked by nextIndex)
			thisServer.leaderStateAttrData.matchIndex[incomingRes.From_CandidateId] =
			thisServer.LOG[thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId].Index];


			//until follower has identical log keep sending next index
			if thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId].Index < (thisServer.lastLogIndex) {

				//thisServer.debug_output2("777",thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId].Index)
				//thisServer.debug_output2("778",thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId].Index)

				nexttobeindex := thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId].Index + 1
				thisServer.leaderStateAttrData.nextIndex[incomingRes.From_CandidateId] = thisServer.LOG[nexttobeindex]


				var appendentrReqObject APPEND_ENTRIES_REQUEST
				appendentrReqObject.LeaderTerm = thisServer.term
				appendentrReqObject.From_CandidateId = thisServer.candidateId
				appendentrReqObject.LeaderCommitIndex = thisServer.commitIndex
				appendentrReqObject.To_CandidateId = incomingRes.From_CandidateId
				appendentrReqObject.LeaderLastLog = thisServer.LOG[nexttobeindex-1]
				appendentrReqObject.LogToAdd =  thisServer.LOG[nexttobeindex]

				actions = append(actions, appendentrReqObject)



			}

		}

	}
	//actions = append(actions,actionElem)

	return actions
}

func (thisServer *SERVER_DATA) commitCheck() (actionElem interface{}) {

	counter := 0



	//actionElem = nil
	//intializing with current commit index , but if any latest thing commits this will change at lineNO 501
	//actionElem = COMMIT_TO_CLIENT{Index:thisServer.commitIndex,
	//Data:thisServer.LOG[thisServer.commitIndex].Data, Err_code:0}
	//
	thisServer.debug_output2("matchindexprint>>>", thisServer.leaderStateAttrData.matchIndex)
	thisServer.debug_output2("commitindex>>>", thisServer.commitIndex)
	thisServer.debug_output2("lastlogindex>>>", thisServer.lastLogIndex)
	thisServer.temp = thisServer.lastLogIndex//will check from leader lastlog to leader commited log, in the next for loop
	thisServer.debug_output2("lastlogindex,copy >>>", thisServer.temp)
	temp := thisServer.commitIndex

	for {
		thisServer.debug_output2("lastlogindex,copyf >>>", thisServer.temp)
		counter = 0
		thisServer.debug_output2("lastlogindex,copyf >>>", thisServer.temp)
		for i := 0; i < len(thisServer.candidates); i++ {

			if thisServer.candidateId == int64(i) {
				continue
			}

			if thisServer.leaderStateAttrData.matchIndex[thisServer.candidates[i]].Index > thisServer.temp || thisServer.leaderStateAttrData.matchIndex[thisServer.candidates[i]].Index == thisServer.temp {
				if thisServer.leaderStateAttrData.matchIndex[thisServer.candidates[i]].Term == thisServer.term {
					counter++; //fmt.Println(i)
					thisServer.debug_output2("ctr++>>>", counter)
					thisServer.debug_output2("looop:", thisServer.temp)
				}
			}
		}
		if (thisServer.LOG[thisServer.lastLogIndex].Index > thisServer.temp || thisServer.LOG[thisServer.lastLogIndex].Index == thisServer.temp) &&
		(thisServer.LOG[thisServer.lastLogIndex].Term == thisServer.term) {

			counter++; thisServer.debug_output2("ctr++>>>", counter)

		}
		thisServer.debug_output2("majcnt", majorityCount(thisServer.candidates))
		//if majority has matched index then update commit index
		if (counter) >= int(majorityCount(thisServer.candidates)) {
			thisServer.commitIndex = thisServer.temp; //send commit to client here
			thisServer.debug_output2("wowcmt:", thisServer.temp)
			//fmt.Println(indexUnderCheck)
			actionElem = COMMIT_TO_CLIENT{Index:thisServer.commitIndex,
				Data:thisServer.LOG[thisServer.commitIndex].Data, Err_code:0}
			break;

		}else {

			if thisServer.temp > thisServer.commitIndex {
				thisServer.temp --;
			}else {

				break
			}
		}

	}
	if thisServer.commitIndex == temp {

		actionElem = nil
	}



	return actionElem
}
//client calls this append for request
func (thisServer *SERVER_DATA) append(data []byte) (actions []interface{}) {


	thisServer.debug_output2("Logger:", thisServer.LOG)
	//fmt.Println(thisServer.LOG)

	//	var actionElem interface{}
	actions = make([]interface{}, 0)

	if thisServer.state == LEADER {


		//add to own log first , its ad to the in-memory map for now
		thisServer.addThisEntry(SERVER_LOG_DATASTR{Index:thisServer.lastLogIndex + 1, Term:thisServer.term, Data:data})
		//here the logstore operation action is added
		actions = append(actions, SERVER_LOG_DATASTR{Index:thisServer.lastLogIndex, Term:thisServer.term, Data:data})
		thisServer.debug_output2("nextindexstr:::", thisServer.leaderStateAttrData.nextIndex)
		//actionElem = thisServer.commitCheck()


		//prepare to send to others

		thisServer.debug_output2("serverLog:", thisServer.LOG)
		for i := 0; i < len(thisServer.candidates); i++ {

			if thisServer.candidates[i] == thisServer.candidateId {
				continue
			}
			var appendentrReqObject APPEND_ENTRIES_REQUEST
			appendentrReqObject.LeaderTerm = thisServer.term
			appendentrReqObject.From_CandidateId = thisServer.candidateId
			appendentrReqObject.LeaderCommitIndex = thisServer.commitIndex
			appendentrReqObject.To_CandidateId = (thisServer.candidates[i])
			nextIndex := thisServer.leaderStateAttrData.nextIndex[(thisServer.candidates[i])].Index

			thisServer.debug_output2("nextindex:", nextIndex)

			appendentrReqObject.LeaderLastLog = thisServer.LOG[nextIndex - 1]
			appendentrReqObject.LogToAdd = thisServer.LOG[nextIndex]

			actions = append(actions, appendentrReqObject)

		}


	}else if thisServer.state == CANDIDATE || thisServer.state == FOLLOWER {

		//dont redirect to last known leader , instead err to client
		//actions = append(actions,REDIRECT_APPEND_DATA{data:data,redirectToiId:thisServer.leaderId})
		actions = append(actions, COMMIT_TO_CLIENT{Data:data, Err_code:ERR_NOT_LEADER})

	}

	//actions = append(actions,actionElem)

	return actions
}

