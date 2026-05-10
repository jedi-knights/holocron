package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jedi-knights/holocron/cli/internal/clienttls"
)

// runCluster dispatches `cluster <subcommand>`.
func runCluster(args []string) error {
	if len(args) == 0 {
		return errors.New("cluster: subcommand required (members | status | join | leave)")
	}
	switch args[0] {
	case "members":
		return runClusterMembers(args[1:])
	case "status":
		return runClusterStatus(args[1:])
	case "join":
		return runClusterJoin(args[1:])
	case "leave":
		return runClusterLeave(args[1:])
	}
	return fmt.Errorf("cluster: unknown subcommand %q", args[0])
}

// runClusterStatus reports the responding broker's Raft leader
// view — this node's ID, whether it holds leadership, and the
// leader it last observed. Pairs with `cluster members` (which
// lists membership) so an operator can answer "where's the
// leader?" in one call.
func runClusterStatus(args []string) error {
	fs := flag.NewFlagSet("cluster status", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	jsonOut := fs.Bool("json", false, "emit machine-readable JSON")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	opts, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, opts...)
	if err != nil {
		return err
	}
	defer tr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	st, err := tr.ClusterStatus(ctx)
	if err != nil {
		return fmt.Errorf("cluster status: %w", err)
	}
	if *jsonOut {
		return printJSON(st)
	}
	if st.NodeID == "" {
		fmt.Println("(broker is not part of a cluster)")
		return nil
	}
	fmt.Printf("node_id:     %s\n", st.NodeID)
	fmt.Printf("is_leader:   %t\n", st.IsLeader)
	if st.LeaderID != "" {
		fmt.Printf("leader_id:   %s\n", st.LeaderID)
		fmt.Printf("leader_addr: %s\n", st.LeaderAddr)
	} else {
		fmt.Println("leader:      (election in progress)")
	}
	return nil
}

func runClusterMembers(args []string) error {
	fs := flag.NewFlagSet("cluster members", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:9092", "broker address")
	apiKey := fs.String("api-key", "", "API key for handshake")
	timeout := fs.Duration("timeout", 5*time.Second, "RPC timeout")
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	opts, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, opts...)
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
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" || *peerAddr == "" {
		return errors.New("cluster join: --id and --peer-addr are required")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	opts, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, opts...)
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
	tlsCfg := clienttls.RegisterFlags(fs)
	credFile := fs.String("credential-file", os.Getenv("HOLOCRON_CREDENTIAL_FILE"), "path to a JWT file (mutually exclusive with --api-key)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *id == "" {
		return errors.New("cluster leave: --id is required")
	}

	cfg, err := tlsCfg()
	if err != nil {
		return err
	}
	opts, err := credentialOpts(*credFile, *apiKey, dialOpts(cfg)...)
	if err != nil {
		return err
	}
	tr, err := dial(*addr, opts...)
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
