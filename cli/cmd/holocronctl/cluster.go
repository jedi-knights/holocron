package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"
)

// runCluster dispatches `cluster <subcommand>`.
func runCluster(args []string) error {
	if len(args) == 0 {
		return errors.New("cluster: subcommand required (members | join | leave)")
	}
	switch args[0] {
	case "members":
		return runClusterMembers(args[1:])
	case "join":
		return runClusterJoin(args[1:])
	case "leave":
		return runClusterLeave(args[1:])
	}
	return fmt.Errorf("cluster: unknown subcommand %q", args[0])
}

func runClusterMembers(args []string) error {
	fs := flag.NewFlagSet("cluster members", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	members, err := tr.ClusterMembers(ctx)
	if err != nil {
		return fmt.Errorf("cluster members: %w", err)
	}
	if len(members) == 0 {
		fmt.Println("(broker is not part of a cluster)")
		return nil
	}
	for _, m := range members {
		fmt.Printf("%s\t%s\n", m.ID, m.Addr)
	}
	return nil
}

func runClusterJoin(args []string) error {
	fs := flag.NewFlagSet("cluster join", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "leader address (the broker the AddVoter request hits)")
	apiKey := fs.String("api-key", "", "API key for handshake")
	id := fs.String("id", "", "ID of the voter to add (required)")
	peerAddr := fs.String("peer-addr", "", "Raft RPC address of the peer (required)")
	timeout := fs.Duration("timeout", 10*time.Second, "RPC timeout (AddVoter blocks until commit)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *peerAddr == "" {
		return errors.New("cluster join: --id and --peer-addr are required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := tr.AddVoter(ctx, *id, *peerAddr); err != nil {
		return fmt.Errorf("cluster join: %w", err)
	}
	fmt.Printf("voter %q added at %s\n", *id, *peerAddr)
	return nil
}

func runClusterLeave(args []string) error {
	fs := flag.NewFlagSet("cluster leave", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "leader address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	id := fs.String("id", "", "ID of the voter to remove (required)")
	timeout := fs.Duration("timeout", 10*time.Second, "RPC timeout")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("cluster leave: --id is required")
	}

	tr, err := dial(*addr, *apiKey)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	if err := tr.RemoveVoter(ctx, *id); err != nil {
		return fmt.Errorf("cluster leave: %w", err)
	}
	fmt.Printf("voter %q removed\n", *id)
	return nil
}
