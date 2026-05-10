// Command holocronctl is the operator CLI for inspecting topics, tailing
// logs, and managing broker state. It talks to the broker via the SDK so
// it never reaches into broker internals.
package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/debug"
)

const usage = `usage: holocronctl <command> [options]

Commands:
  topic create      Create a topic on the broker
  topic delete      Delete a topic and every record on it
  topic describe    Show one topic's full configuration
  topic list        Enumerate every topic on the broker
  topic stats       Per-partition record counts for a topic
  topic update      Change a topic's retention or segment size
  produce           Send one record to a topic
  consume           Read records from a topic and print them
  tail              Print records arriving at a partition's high-water
  record fetch      Read one record by (topic, partition, offset)
  bench             Produce N records and report throughput + latency
  group list        Enumerate consumer groups (name, generation, members)
  group describe    Show per-member partition assignments for a group
  offset commit     Commit a group's offset on a partition (next-to-read)
  offset reset      Reset a group's offset on a partition to 0
  cluster members   List Raft voters
  cluster status    Show this node's Raft state and leader info
  cluster join      Add a voter to the cluster (leader-only)
  cluster leave     Remove a voter (leader-only)
  auth issue        Sign a JWT with the operator's Ed25519 private key
  auth inspect      Decode a JWT and print its header + claims (no verification)

  --version         Print build version and Go runtime info

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

// printVersion writes build info — module version (when built
// with go install / go build from a tagged module) and Go runtime
// version — to stdout. Uses runtime/debug.ReadBuildInfo so the
// output stays accurate without manual ldflag wiring.
func printVersion() {
	version := "(devel)"
	commit := ""
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" {
			version = info.Main.Version
		}
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				commit = s.Value
				break
			}
		}
	}
	fmt.Printf("holocronctl %s\n", version)
	if commit != "" {
		fmt.Printf("commit:  %s\n", commit)
	}
	fmt.Printf("go:      %s\n", runtime.Version())
	fmt.Printf("os/arch: %s/%s\n", runtime.GOOS, runtime.GOARCH)
}

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}
	if args[0] == "--version" || args[0] == "version" {
		printVersion()
		return nil
	}
	commands := []command{
		{"topic", runTopic},
		{"produce", runProduce},
		{"consume", runConsume},
		{"cluster", runCluster},
		{"group", runGroup},
		{"offset", runOffset},
		{"record", runRecord},
		{"bench", runBench},
		{"tail", runTail},
		{"ping", runPing},
		{"auth", runAuth},
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
