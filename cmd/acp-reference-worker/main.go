// acp-reference-worker is the single-job R5 child, not a general command agent.
package main

import (
	"context"
	"os"

	"github.com/dilukhin/agent_control_plane/internal/protocol"
	"github.com/dilukhin/agent_control_plane/internal/transport/process"
)

func main() {
	if len(os.Args) != 4 || os.Args[1] != "--acp-worker-v1" {
		os.Exit(2)
	}
	if err := process.Serve(context.Background(), os.Stdin, os.Stdout, protocol.ActorID(os.Args[2]), os.Args[3]); err != nil {
		os.Exit(1)
	}
}
