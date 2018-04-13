package core

import (
	"net"
	"google.golang.org/grpc"
	"sync"
	"errors"
	"context"
	pb "proto"
	"log"
)

type GRPCServer interface {
	Address() string
	Start(n *Node) error
	Stop()
	Server() *grpc.Server
	Listener() net.Listener
	//RequestVote(context.Context, *pb.VoteRequest) (*pb.VoteRespond, error)
	//AppendEntries(context.Context, *pb.EntryRequest) (*pb.EntryResponse, error)
}

type grpcServerImpl struct {
	//Listen address for the server specified as hostname:port
	address string
	//Listener for handling network requests
	listener net.Listener
	//GRPC server
	server *grpc.Server
	//lock to protect concurrent access to append / remove
	lock *sync.Mutex
	node *Node
}

func NewGRPCServer(address string) (GRPCServer, error) {

	if address == "" {
		return nil, errors.New("Missing address parameter")
	}
	//create our listener
	lis, err := net.Listen("tcp", address)

	if err != nil {
		return nil, err
	}
	log.Println("listen to address "+address)
	return NewGRPCServerFromListener(lis)

}

func NewGRPCServerFromListener(listener net.Listener) (GRPCServer, error) {

	grpcServer := &grpcServerImpl{
		address:  listener.Addr().String(),
		listener: listener,
		lock:     &sync.Mutex{},
	}

	var serverOpts []grpc.ServerOption

	serverOpts = append(serverOpts, grpc.MaxSendMsgSize(1024*1024))
	serverOpts = append(serverOpts, grpc.MaxRecvMsgSize(1024*1024))

	grpcServer.server = grpc.NewServer(serverOpts...)
	pb.RegisterMgrServer(grpcServer.server,grpcServer)

	return grpcServer, nil
}

//Address returns the listen address for this GRPCServer instance
func (gServer *grpcServerImpl) Address() string {
	return gServer.address
}

//Listener returns the net.Listener for the GRPCServer instance
func (gServer *grpcServerImpl) Listener() net.Listener {
	return gServer.listener
}

//Server returns the grpc.Server for the GRPCServer instance
func (gServer *grpcServerImpl) Server() *grpc.Server {
	return gServer.server
}

func (gServer *grpcServerImpl) Start(n *Node) error {
	gServer.node = n
	log.Println("start server by rpc")
	go gServer.server.Serve(gServer.listener)
	return nil
}

//Stop stops the underlying grpc.Server
func (gServer *grpcServerImpl) Stop() {
	gServer.server.Stop()
}

func (gServer *grpcServerImpl) RequestVote(ctx context.Context, vr *pb.VoteRequest) (*pb.VoteRespond, error) {
	vresp, err := gServer.node.RequestVote(*vr)
	return &vresp, err
}
func (gServer *grpcServerImpl) AppendEntries(ctx context.Context, er *pb.EntryRequest) (*pb.EntryResponse, error) {
	eresp, err := gServer.node.AppendEntries(*er)
	return &eresp, err
}
