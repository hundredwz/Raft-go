package core

import (
	"sync"
	pb "proto"
	"time"
	"math/rand"
	"log"
	"bytes"
)

const (
	FOLLOWER  = iota
	CANDIDATE
	LEADER
)

type Node struct {
	sync.RWMutex

	ID              string
	Server          GRPCServer
	Client          GRPCClient
	Status          int
	Log             Logger
	StateMachine    Applyer
	ElectionTimeout time.Duration
	Uncommitted     map[int64]*CommandRequest

	Term         int64
	ElectionTerm int64
	VoteFor      string
	Ballots      int
	Cluster      []*Peer

	endElectionChan      chan int
	finishedElectionChan chan int

	VoteResponseChan          chan pb.VoteRespond
	RequestVoteChan           chan pb.VoteRequest
	RequestVoteResponseChan   chan pb.VoteRespond
	AppendEntriesChan         chan pb.EntryRequest
	AppendEntriesResponseChan chan pb.EntryResponse
	CommandChan               chan CommandRequest
	ExitChan                  chan int
}

func NewNode(id string, client GRPCClient, server GRPCServer, logger Logger, applyer Applyer) *Node {
	node := &Node{
		ID:              id,
		Server:          server,
		Client:          client,
		Log:             logger,
		Status:          FOLLOWER,
		StateMachine:    applyer,
		ElectionTimeout: 500 * time.Millisecond,
		Uncommitted:     make(map[int64]*CommandRequest),

		VoteResponseChan:          make(chan pb.VoteRespond),
		RequestVoteChan:           make(chan pb.VoteRequest),
		RequestVoteResponseChan:   make(chan pb.VoteRespond),
		AppendEntriesChan:         make(chan pb.EntryRequest),
		AppendEntriesResponseChan: make(chan pb.EntryResponse),
		CommandChan:               make(chan CommandRequest),
		ExitChan:                  make(chan int),
	}
	return node
}

func (n *Node) Serve() error {
	err := n.Server.Start(n)
	log.Println("serve start ")
	go n.loop()
	return err
}

func (n *Node) Stop() {
	n.Server.Stop()
}

func (n *Node) AddToCluster(member string) {
	p := &Peer{
		ID: member,
	}
	n.Cluster = append(n.Cluster, p)
}

func (n *Node) RequestVote(vr pb.VoteRequest) (pb.VoteRespond, error) {
	n.RequestVoteChan <- vr
	return <-n.RequestVoteResponseChan, nil
}

func (n *Node) AppendEntries(er pb.EntryRequest) (pb.EntryResponse, error) {
	n.AppendEntriesChan <- er
	return <-n.AppendEntriesResponseChan, nil
}

func (n *Node) randomElectionTimeout() time.Duration {
	return n.ElectionTimeout + time.Duration(rand.Int63n(int64(n.ElectionTimeout)))
}

func (n *Node) loop() {
	followerTimer := time.NewTicker(200 * time.Millisecond)
	electionTimer := time.NewTimer(n.randomElectionTimeout())
	for {
		select {
		case <-electionTimer.C:
			log.Println("election timer")
			n.electionTimeout()
		case <-followerTimer.C:
			n.updateFollowers()
			continue
		case vreq := <-n.RequestVoteChan:
			vresp, _ := n.doRequestVote(vreq)
			n.RequestVoteResponseChan <- vresp
		case vresp := <-n.VoteResponseChan:
			log.Println(n.ID + " DO response for vote")
			n.doRespondVote(vresp)
		case areq := <-n.AppendEntriesChan:
			aresp, _ := n.doAppendEntries(areq)
			n.AppendEntriesResponseChan <- aresp
		case creq := <-n.CommandChan:
			n.doCommand(creq)
			n.updateFollowers()
		case <-n.ExitChan:
			n.stepDown()
			goto exit
		}
		randElectionTimeout := n.randomElectionTimeout()
		if !electionTimer.Reset(randElectionTimeout) {
			electionTimer = time.NewTimer(randElectionTimeout)
		}
	}
exit:
	log.Printf("[%s] exiting ioLoop()", n.ID)
}

func (n *Node) electionTimeout() {
	log.Println(n.ID + " electionTimeout")
	if n.Status == LEADER {
		return
	}
	n.Status = CANDIDATE
	n.endElection()
	n.nextTerm()
	n.runForLeader()
}

func (n *Node) endElection() {
	log.Println(n.ID + " endElection")
	if n.Status != CANDIDATE || n.endElectionChan == nil {
		return
	}
	close(n.endElectionChan)
	<-n.finishedElectionChan

	n.endElectionChan = nil
	n.finishedElectionChan = nil

}
func (n *Node) setTerm(term int64) {
	// check freshness
	if term <= n.Term {
		return
	}

	n.Term = term
	n.Status = FOLLOWER
	n.VoteFor = ""
	n.Ballots = 0
}
func (n *Node) nextTerm() {
	n.Status = FOLLOWER
	n.Term++
	n.Ballots = 0
}
func (n *Node) stepDown() {
	if n.Status == FOLLOWER {
		return
	}

	log.Printf("[%s] stepDown()", n.ID)

	n.Status = FOLLOWER
	n.VoteFor = ""
	n.Ballots = 0
}

func (n *Node) promoteToLeader() {
	n.Status = LEADER
	log.Println(n.ID + " chosen as Leader")
	for _, peer := range n.Cluster {
		peer.NextIndex = n.Log.Index()
	}
}

func (n *Node) runForLeader() {
	log.Println(n.ID + " run for leader")
	n.Status = CANDIDATE
	n.Ballots++
	n.ElectionTerm = n.Term
	n.endElectionChan = make(chan int)
	n.finishedElectionChan = make(chan int)
	vr := &pb.VoteRequest{
		Term:         n.Term,
		CandidateID:  n.ID,
		LastLogIndex: n.Log.Index(),
		LastLogTerm:  n.Log.Term()}
	go n.gatherVotes(vr)
}

func (n *Node) updateFollowers() {
	var er *pb.EntryRequest
	if n.Status != LEADER {
		return
	}

	for _, peer := range n.Cluster {
		if n.Log.LastIndex() < peer.NextIndex {
			// heartbeat
			_, prevLogIndex, prevLogTerm := n.Log.GetEntryForRequest(n.Log.LastIndex())

			er = &pb.EntryRequest{
				CmdID:        -1,
				Term:         n.Term,
				LeaderID:     n.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Data:         []byte("")}
		} else {
			entry, prevLogIndex, prevLogTerm := n.Log.GetEntryForRequest(peer.NextIndex)
			er = &pb.EntryRequest{
				CmdID:        entry.CmdID,
				Term:         n.Term,
				LeaderID:     n.ID,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Data:         entry.Data}
		}
		_, err := n.Client.AppendEntries(peer.ID, er)
		if err != nil {
			peer.NextIndex--
			if peer.NextIndex < 0 {
				peer.NextIndex = 0
			}
			continue
		}

		if er.CmdID == -1 {
			// skip commit checks for heartbeats
			continue
		}

		// TODO: keep track of CommitIndex and send to followers

		// TODO: keep track of whether or not the leader has propagated an entry for the *current* term

		// A log entry may only be considered committed if the entry is stored on a majority of the
		// servers; in addition, at least one entry from the leader’s current term must also be stored
		// on a majority of the servers.
		majority := int32((len(n.Cluster)+1)/2 + 1)
		cr := n.Uncommitted[er.CmdID]
		cr.ReplicationCount++
		if cr.ReplicationCount >= majority && cr.State != Committed {
			cr.State = Committed
			log.Printf("[%s] !!! apply %+v", n.ID, cr)
			err := n.StateMachine.Apply(cr)
			if err != nil {
				// TODO: what do we do here?
			}
			cr.ResponseChan <- CommandResponse{LeaderID: n.VoteFor, Success: true}
		}
		peer.NextIndex++
	}
}

func (n *Node) doRequestVote(vr pb.VoteRequest) (pb.VoteRespond, error) {
	if vr.Term < n.Term {
		return pb.VoteRespond{Term: n.Term, VoteGranted: false}, nil
	}
	if vr.Term > n.Term {
		n.endElection()
		n.setTerm(vr.Term)
	}
	if n.VoteFor != "" && n.VoteFor != vr.CandidateID {
		return pb.VoteRespond{Term: n.Term, VoteGranted: false}, nil
	}
	if n.Log.FresherThan(vr.LastLogIndex, vr.LastLogTerm) {
		return pb.VoteRespond{Term: n.Term, VoteGranted: false}, nil
	}
	n.VoteFor = vr.CandidateID
	return pb.VoteRespond{Term: n.Term, VoteGranted: true}, nil
}
func (n *Node) doAppendEntries(er pb.EntryRequest) (pb.EntryResponse, error) {
	if er.Term < n.Term {
		return pb.EntryResponse{n.Term, false}, nil
	}
	if n.Status != FOLLOWER {
		n.endElection()
		n.stepDown()
	}

	if er.Term > n.Term {
		n.setTerm(er.Term)
	}
	err := n.Log.Check(er.PrevLogIndex, er.PrevLogTerm, er.PrevLogIndex+1, er.Term)
	if err != nil {
		return pb.EntryResponse{Term: n.Term, Success: false}, nil
	}
	if bytes.Compare(er.Data, []byte("")) == 0 {
		return pb.EntryResponse{Term: n.Term, Success: true}, nil
	}
	e := &Entry{
		CmdID: er.CmdID,
		Index: er.PrevLogIndex + 1,
		Term:  er.Term,
		Data:  er.Data,
	}
	err = n.Log.Append(e)
	return pb.EntryResponse{Term: n.Term, Success: err == nil}, nil
}
func (n *Node) doRespondVote(vr pb.VoteRespond) {

	if n.Status != CANDIDATE {
		return
	}
	log.Println(n.ID+" STATUS IS ", n.Status, vr.Term, n.ElectionTerm)
	if vr.Term != n.ElectionTerm {
		if vr.Term > n.ElectionTerm {
			// we discovered a higher term
			n.endElection()
			n.setTerm(vr.Term)
		}
		return
	}

	if vr.VoteGranted {
		n.voteGranted()
	}

}
func (n *Node) doCommand(cr CommandRequest) {
	if n.Status != LEADER {
		cr.ResponseChan <- CommandResponse{LeaderID: n.VoteFor, Success: false}
	}

	e := &Entry{
		CmdID: cr.ID,
		Index: n.Log.Index(),
		Term:  n.Term,
		Data:  cr.Body,
	}

	err := n.Log.Append(e)
	if err != nil {
		cr.ResponseChan <- CommandResponse{LeaderID: n.VoteFor, Success: false}
	}

	// TODO: should check uncommitted before re-appending, etc.
	cr.State = Logged
	cr.ReplicationCount++
	n.Uncommitted[cr.ID] = &cr
}

func (n *Node) gatherVotes(request *pb.VoteRequest) {
	for _, peer := range n.Cluster {
		go func(p string) {
			vresp, err := n.Client.RequestVote(p, request)
			log.Println(n.ID + " recv result ")
			log.Println(vresp)
			if err != nil {
				log.Printf("[%s] error in RequestVoteRPC() to %s - %s", n.ID, p, err)
				return
			}
			n.VoteResponseChan <- *vresp
		}(peer.ID)
	}

	<-n.endElectionChan
	close(n.finishedElectionChan)
}
func (n *Node) voteGranted() {
	n.Ballots++

	majority := (len(n.Cluster)+1)/2 + 1
	log.Println(n.ID+" BALLOT number is ", n.Ballots, majority)
	if n.Ballots >= majority {
		n.endElection()
		n.promoteToLeader()
	}
}
