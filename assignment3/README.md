# cs733
cs 733 project (UPDATE 4.0)

#THE RAFT NODES 

Description
-----------

The raft Node machine and state machine is built on the principles provided in the paper ``` In Search of an Understandable Consensus Algorithm (Extended Version) by Diego Ongaro and John Ousterhout. ``` 

This raft node implements network disk io on top of the functionalities of the Raft state machine.

**WORKING**

Raft node consists of 5 raft nodes which are independent in function and pass states and messages only through message.

Systems starts up with 5 node initialization with some configuration and partition information

System stabilises with a leader and gets ready to accept any append request from the client, as soon as the client sends a message, raft system logs it and replicates it with all nodes and also sends out commit flags once its replicated on that respective node.

Raft expose API
---------------
```

// Client's message to Raft node
Append([]byte)


// A channel for client to listen on. What goes into Append must come out of here at some point.
CommitChannel() 

// Last known committed index in the log.
CommittedIndex() int
This could be -1 until the system stabilizes.


// Returns the data at a log index, or an error.
Get(index int) (err, []byte)


// Node's id
Id()// Id of leader. -1 if unknown


LeaderId() int

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
Shutdown()

}
```


UNIT TEST 
----------
**State machine test**

1) This release provides an updated and corrected testcases for the internal raft state machine which ensures 100 % code coverage

2) Exclusively covers testing of all possible producible scenarios with easy timeout and event configurations

3) Tests consists of all possible message handling for all types of states

**Raft node test**

1) Raft node tests covers the basic test for replication of log

2) Tests quick consecutive log addition to the leader to see if it gets replicated in some time

3) Tests a partition simulation which creates network partition of nodes and adds some logs to the active partition and checks if they could elect a leader and progress.

4) Test also covers scenario of healing the partition and check for consistency of all the logs

***
***
***
***
***
___
