package core

import (
	pb "proto"
	"log"
	"google.golang.org/grpc"
	"context"
)

type GRPCClient interface {
	RequestVote(address string, request *pb.VoteRequest) (*pb.VoteRespond, error)
	AppendEntries(address string, request *pb.EntryRequest) (*pb.EntryResponse, error)
}

type GRPCClientImpl struct {
	opts []grpc.DialOption
}

func NewGRPCClient(opts ...grpc.DialOption) (GRPCClient, error) {
	opts = append(opts, grpc.WithInsecure())
	client := &GRPCClientImpl{opts: opts}
	return client, nil
}

func (c *GRPCClientImpl) RequestVote(address string, request *pb.VoteRequest) (*pb.VoteRespond, error) {
	conn, err := grpc.Dial(address, c.opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewMgrClient(conn)
	resp, err := client.RequestVote(context.Background(), request)
	log.Println("gather error "+err.Error())
	return resp, err

}

func (c *GRPCClientImpl) AppendEntries(address string, request *pb.EntryRequest) (*pb.EntryResponse, error) {
	conn, err := grpc.Dial(address, c.opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()
	client := pb.NewMgrClient(conn)
	resp, err := client.AppendEntries(context.Background(), request)
	return resp, err
}
