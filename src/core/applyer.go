package core

type Applyer interface {
	Apply(cmd *CommandRequest) error
}

const (
	Initialized = iota
	Logged
	Committed
)

type CommandRequest struct {
	ID               int64  `json:"id"`
	Name             string `json:"name"`
	Body             []byte `json:"body"`
	ResponseChan     chan CommandResponse
	ReplicationCount int32
	State            int32
}

type CommandResponse struct {
	LeaderID string
	Success  bool
}

type StateMachine struct {
}

func (s *StateMachine) Apply(cr *CommandRequest) error {
	return nil
}