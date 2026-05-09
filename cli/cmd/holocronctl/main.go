// Command holocronctl is the operator CLI for inspecting topics, tailing
// logs, and managing broker state. It talks to the broker via the SDK so
// it never reaches into broker internals.
package main

import (
	"fmt"
	"os"
)

const usage = `usage: holocronctl <command> [options]

Commands:
  topic create     Create a topic on the broker
  topic list       List topics by name (probes via Metadata)
  produce          Send one record to a topic
  consume          Read records from a topic and print them
  cluster members  List Raft voters
  cluster join     Add a voter to the cluster (leader-only)
  cluster leave    Remove a voter (leader-only)

Run 'holocronctl <command> -h' for command-specific options.
`

// command names a top-level subcommand. Each is dispatched through a
// dedicated handler that owns its own flag.FlagSet.
type command struct {
	name string
	run  func(args []string) error
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "holocronctl:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	commands := []command{
		{"topic", runTopic},
		{"produce", runProduce},
		{"consume", runConsume},
		{"cluster", runCluster},
	}
	for _, c := range commands {
		if c.name == args[0] {
			return c.run(args[1:])
		}
	}
	if args[0] == "-h" || args[0] == "--help" {
		fmt.Print(usage)
		return nil
	}
	return fmt.Errorf("unknown command %q (run 'holocronctl' for usage)", args[0])
}
