# cs733
cs 733 project

#THE RAFT STATE MACHINE

Description
-----------

The raft state machine is built on the principles provided in the paper ``` In Search of an Understandable Consensus Algorithm (Extended Version) by Diego Ongaro and John Ousterhout. ``` 

This state machine covers only the protocol part of the consensus algorithm and doesnt implement network disk io.


UNIT TEST 
----------
Unit test provided with the package checks for a 100% code coverage , that ensures correctness of all scenarios .The test also demonstrates that the implementation is race free.

Future work
-----------
Future work will have the uper layer involving Network and Disk io which will use the raft state machine in its core.


Description
-----------

Course project on a distributed file server which can store and serv file on demand and supports concurrent access from multiple 
client.


Usage
---------

The project supports READ, WRITE, COMPARE AND SWAP AND DELETE OPERATION


####WRITE SYNTAX:
  REQUEST:```write filename numbytes [exptime]\r\n<content bytes>\r\n```

  RESPONSE:```OK version\r\n```

####READ SYNTAX:
  REQUEST:```read filename\r\n```

  RESPONSE:```CONTENTS version numbytes exptime> \r\n<content bytes>\r\n```

####COMPARE AND SWAP SYNTAX:

REQUEST:```cas filename version numbytes exptime\r\ncontent_bytes\r\n```

RESPONSE:```OK version\r\n```

####DELETE SYNTAX:

REQUEST:```delete filename\r\n```

RESPONSE:```OK\r\n```


####ERROR CODES:
1.ERR_VERSION - Content specified in cas doesnt match with the version of the file

2.ERR_FILE_NOT_FOUND - File with the provide name doesnt exist or has expired

3.ERR_CMD_ERR - Command provided is not in proper format

4.ERR_INTERNAL - internal server error


UNIT TEST 
----------
Unit test provided with the package checks for sequential execution of each type of command and checks for expected outputs
Test contains a segment for concurrent execution of 10 write process to ensure that mutual exclusion is properly handled.

Future work
-----------
Future work will have more functionalities and extensive tests to ensure quality


