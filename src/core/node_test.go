package core

import (
	"time"
	"log"
	"os"
	"io/ioutil"
	"testing"
	"strconv"
)

func genNodes(num int) []*Node {
	nodes := make([]*Node, 0)
	for i := 0; i < num; i++ {
		client, _ := NewGRPCClient()
		server, _ := NewGRPCServer("127.0.0.1:" + strconv.Itoa(8000+i*10))
		logger := &Log{}
		stateMachine := &StateMachine{}
		n := NewNode(strconv.Itoa(i), client, server, logger, stateMachine)
		nodes = append(nodes, n)
	}
	time.Sleep(100 * time.Millisecond)
	for i := 0; i < len(nodes); i++ {
		for j := 0; j < len(nodes); j++ {
			if j != i {
				nodes[i].AddToCluster(nodes[j].Server.Address())
			}
		}
	}
	for _, node := range nodes {
		log.Println("start range")
		node.Serve()
	}
	return nodes

}

func countLeaders(nodes []*Node) int {
	leaders := 0
	for i := 0; i < len(nodes); i++ {
		nodes[i].RLock()
		if nodes[i].Status == LEADER {
			leaders++
		}
		nodes[i].RUnlock()
	}
	return leaders
}

func findLeader(nodes []*Node) *Node {
	for i := 0; i < len(nodes); i++ {
		nodes[i].RLock()
		if nodes[i].Status == LEADER {
			nodes[i].RUnlock()
			return nodes[i]
		}
		nodes[i].RUnlock()
	}
	return nil
}

func startCluster(num int) ([]*Node, *Node) {
	nodes := genNodes(num)
	for {
		time.Sleep(50 * time.Millisecond)
		if countLeaders(nodes) == 1 {
			break
		}
	}
	leader := findLeader(nodes)
	return nodes, leader
}

func stopCluster(nodes []*Node) {
	for _, node := range nodes {
		node.Stop()
	}
}

func TestStartup(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes, _ := startCluster(3)
	defer stopCluster(nodes)
	time.Sleep(250 * time.Millisecond)
	if countLeaders(nodes) != 1 {
		t.Fatalf("leaders should still be one")
	}
}

func TestNodeKill(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes, leader := startCluster(3)
	defer stopCluster(nodes)
	leader.Stop()
	for {
		time.Sleep(50 * time.Millisecond)
		if countLeaders(nodes) == 1 {
			break
		}
	}
}

func TestCommand(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	log.SetOutput(os.Stdout)

	nodes, leader := startCluster(5)
	defer stopCluster(nodes)
	for i := int64(0); i < 10; i++ {
		responseChan := make(chan CommandResponse, 1)
		cr := CommandRequest{
			ID:           i + 1,
			Name:         "SUP",
			Body:         []byte("BODY"),
			ResponseChan: responseChan,
		}
		leader.doCommand(cr)
		<-responseChan
	}

	time.Sleep(10 * time.Second)
}


