package httpapi

import (
	"github.com/peterbourgon/caspaxos"
)

var _ caspaxos.Acceptor = (*AcceptorClient)(nil)
