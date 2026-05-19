package dto

import "encoding/json"

type ErrProposeCmd struct {
	err        error
	state      string
	leader     string
	hostLeader string
}

func NewErrProposeCmd(err error, state, leader string) *ErrProposeCmd {
	return &ErrProposeCmd{
		err:    err,
		state:  state,
		leader: leader,
	}
}

func (rp ErrProposeCmd) Err() error {
	return rp.err
}

func (rp ErrProposeCmd) RespJson() []byte {
	b, _ := json.Marshal(map[string]any{
		"error":  rp.err.Error(),
		"state":  rp.state,
		"leader": rp.leader,
	})

	return b
}
