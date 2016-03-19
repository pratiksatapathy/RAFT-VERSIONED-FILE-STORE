package main

import (
	"fmt"
	"github.com/cs733-iitb/cluster"
	"github.com/cs733-iitb/cluster/mock"
	"time"
	"testing"
	"os"
)

var rafts []RaftNode
var mckk *mock.MockCluster
func init() {

	initialization()
}
func initialization() {
	cleanup()//cleanup before begining

	fmt.Print("TEST TAKES APPROX 75 SECS/ PLEASE HOLD ON")
	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
		{Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
	}}
	cluster, err := mock.NewCluster(clconfig)
	mckk = cluster
	rafts = rafter(cluster) // array of []raft.Node

	check(err)
	time.Sleep(10 * time.Second)

}

func cleanup() {

	for i := 0; i < len(rafts); i++ {
		rafts[i].ShutDown()
		//time.Sleep(1 * time.Second)

	}

	time.Sleep(1 * time.Second)

//	for i := 0; i < len(rafts); i++ {
//		rafts[i] = RaftNode{}
//	}

	err := os.RemoveAll("dir1");os.RemoveAll("dir2");os.RemoveAll("dir3");os.RemoveAll("dir4");os.RemoveAll("dir5")

	check(err)

}

//test provided by sir
func TestBasic(t *testing.T) {
//initialization()
	//var rafts []RaftNode


	//--------------------------subtest 1 adding one entry and checking commit channel


	//time.Sleep(5 * time.Second)
	(rafts[findLeader(rafts) - 1]).Append([]byte("foo"))

	time.Sleep(2 * time.Second)

	for i := 0; i < len(rafts); i++ {
		select {
		// to avoid blocking on channel.
		case ci := <-rafts[i].CommitChannel():
			if ci.Err_code == ERR_NOT_LEADER {
				t.Fatal(ci.Err_code)
			}
			if string(ci.Data) != "foo" {
				t.Fatal("Got different data from %d",rafts[i].Id())
			}
			if rafts[i].CommittedIndex() != 1{
				t.Fatal("commit index mismatch")
			}
		default:
			t.Fatal("Expected message on all nodes")
		}
	}


	//--------------------------subtest 2 adding onemore entry and checking commit channel


	//time.Sleep(10 * time.Second)
	(rafts[findLeader(rafts) - 1]).Append([]byte("foo2"))

	time.Sleep(5 * time.Second)

	for i := 0; i < len(rafts); i++ {
		select {
		// to avoid blocking on channel.
		case ci := <-rafts[i].CommitChan:
			if ci.Err_code == ERR_NOT_LEADER {
				t.Fatal(ci.Err_code)
			}
			if string(ci.Data) != "foo2" {
				t.Fatal("Got different data")
			}
		default:
			t.Fatal("Expected message on all nodes")
		}
	}
}

//adding entries in quick succession
func TestBasicAppendfourmoreentries(t *testing.T) {

	//time.Sleep(10 * time.Second)
	(rafts[findLeader(rafts) - 1]).Append([]byte("cmd1"))
	(rafts[findLeader(rafts) - 1]).Append([]byte("cmd2"))
	(rafts[findLeader(rafts) - 1]).Append([]byte("cmd3"))
	(rafts[findLeader(rafts) - 1]).Append([]byte("cmd4"))

	time.Sleep(4 * time.Second)




	lastlog,err:= rafts[1].GetIndex(rafts[1].LogHandler.GetLastIndex())

	check(err)
	if  string(lastlog) != "cmd4" {
		t.Fatal("log mismatch"); t.Fatal(rafts[1].LogHandler.GetLastIndex())
	}

	//---------------------------check that everyone is aware of the leader

	for i := 0; i < len(rafts); i++ {
		if findLeader(rafts) != int(rafts[i].LeaderId()){
			t.Fatal("unpropagated value for leader id")
		}

	}


	cleanup()
}



func TestSeparationAndHeal(t *testing.T) {
//initialization()
	//---------------------------------------------------subtest 1(test to check whether the partion catches up after join) ------------------------

	clconfig := cluster.Config{Peers:[]cluster.PeerConfig{
		{Id:1}, {Id:2}, {Id:3}, {Id:4}, {Id:5},
	}}
	cluster, err := mock.NewCluster(clconfig)
	check(err)
	cluster.Partition([]int{1, 2, 3}, []int{4, 5}) // Cluster partitions into two.

	rafts := rafter(cluster) // array of []raft.Node

	//time.Sleep(10 * time.Second)
	(rafts[findLeader(rafts) - 1]).Append([]byte("foo"))
	time.Sleep(3 * time.Second)

	for i := 0; i < len(rafts); i++ {

		if i >= 3 {
			if rafts[i].LogHandler.GetLastIndex() != 0 {
				t.Fatal(rafts[i].LogHandler.GetLastIndex())
			}
		}else {
			if rafts[i].LogHandler.GetLastIndex() != 1 {
				t.Fatal(rafts[i].LogHandler.GetLastIndex())
			}
		}

	}

	cluster.Heal()

	time.Sleep(4 * time.Second)
	(rafts[findLeader(rafts) - 1]).Append([]byte("bar"))
	time.Sleep(15 * time.Second)

	for i := 0; i < len(rafts); i++ {
		//		select {
		if rafts[i].LogHandler.GetLastIndex() != 2 {

			t.Fatal(rafts[i].LogHandler.GetLastIndex())
		}

	}

	cleanup()
}
