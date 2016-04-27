
# THE RAFT CONSENSUS ALGORITHM IMPLEMENTATION ON A VOLATILE FILE STORE

cs 733 project course project 

by pratik satapathy

Description
-----------

The raft state machine is built on the principles provided in the paper \
**In Search of an Understandable Consensus Algorithm (Extended Version) by Diego Ongaro and John Ousterhout.**

##### The project consists of two parts 


-  A non-persistent file store (Version file store)
-  Raft consensus protocol implementation


Project Description
-----------

### 1) Version file store

A small but essential part of the project is the distributed version file server which can store and serv file on demand and supports concurrent access from multiple 
client.
We describe the primary usage of the file store command interface below

Usage

---------

The project supports READ, WRITE, COMPARE AND SWAP AND DELETE OPERATION
```sh
WRITE SYNTAX:

  REQUEST:write filename numbytes [exptime]\r\n<content bytes>\r\n
  RESPONSE:OK version\r\n

READ SYNTAX:

  REQUEST:read filename\r\n
  RESPONSE:CONTENTS version numbytes exptime> \r\n<content bytes>\r\n

COMPARE AND SWAP SYNTAX:

  REQUEST:cas filename version numbytes exptime\r\ncontent_bytes\r\n
  RESPONSE:OK version\r\n

####DELETE SYNTAX:

REQUEST:delete filename\r\n
RESPONSE:OK\r\n

```
####ERROR CODES:

```
1.ERR_VERSION - Content specified in cas doesnt match with the version of the file

2.ERR_FILE_NOT_FOUND - File with the provide name doesnt exist or has expired

3.ERR_CMD_ERR - Command provided is not in proper format

4.ERR_INTERNAL - internal server error
```

## 2 ) The RAFT consensus layer
This raft node implements network disk io on top of the functionalities of the Raft state machine.

**WORKING**

Raft node consists of 5 raft nodes which are independent in function and pass states and messages only through message.

Systems starts up with 5 node initialization with some configuration and partition information

System stabilises with a leader and gets ready to accept any append request from the client, as soon as the client sends a message, raft system logs it and replicates it with all nodes and also sends out commit flags once its replicated on that respective node.

Here comes the role of the Version file store. The version file store receives all request given to it and directs them to the consensus layer for reliable replication. Once replication is done, the Raft layer informs the version file store through a commit channel. Based on this, the file store performs the operation and replies to client.

Below we present some of the exposed API that are provided by the RAFT layer for implementation in application which requires distributed reliability. In this case the Version file store.

Raft Exposed API
---------------
```

Client's message to Raft node
    Append([]byte)


A channel for client to listen on. What goes into Append must come out of here at some point.
    CommitChannel() 

Last known committed index in the log.
    CommittedIndex() int
    This could be -1 until the system stabilizes.


Returns the data at a log index, or an error.
    Get(index int) (err, []byte)

Node's id
    Id()// Id of leader. -1 if unknown


LeaderId() int

// Signal to shut down all goroutines, stop sockets, flush log and close it, cancel timers.
Shutdown()

}
```



