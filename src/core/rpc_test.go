package core

import (
	"testing"
	"proto"
	"log"
	"strconv"
)

func TestClient(t *testing.T) {
	client, _ := NewGRPCClient()
	vr := &proto.VoteRequest{
		1, "1", 1, 1,
	}
	resp, _ := client.RequestVote("127.0.0.1:8000", vr)
	log.Println(resp)
}

func TestServer(t *testing.T) {
	server, _ := NewGRPCServer("127.0.0.1:8000")
	client, _ := NewGRPCClient()
	logger := &Log{}
	stateMachine := &StateMachine{}
	n := NewNode(strconv.Itoa(1), client, server, logger, stateMachine)
	n.Serve()
	a := make(chan int)
	<-a
}
