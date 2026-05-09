// Command holocrond is the holocron broker daemon.
//
// Stage 5 keeps the Stage 3 disk + network defaults and adds optional
// Raft-replicated cluster mode behind --cluster. In cluster mode produce
// and topic-create operations replicate across N nodes via hashicorp/raft;
// followers redirect SDK clients to the leader's wire port.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
)

const stage = "5 (cluster)"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "holocrond:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("holocrond", flag.ContinueOnError)
	dataDir := fs.String("data-dir", envOrDefault("HOLOCRON_DATA_DIR", "/var/lib/holocron"), "directory for persistent broker state")
	listen := fs.String("listen", envOrDefault("HOLOCRON_LISTEN", ":9092"), "TCP address to listen on (empty disables the listener)")
	retention := fs.Duration("retention", 0, "delete sealed segments older than this; 0 disables time retention")
	retentionBytes := fs.Int64("retention-bytes", 0, "delete oldest sealed segments while a partition exceeds this size; 0 disables size retention")
	memory := fs.Bool("memory", false, "use the in-memory store instead of disk (testing only)")
	clusterMode := fs.Bool("cluster", false, "enable Raft-replicated cluster mode")
	nodeID := fs.String("node-id", envOrDefault("HOLOCRON_NODE_ID", ""), "this node's ID (cluster mode)")
	raftBind := fs.String("raft-listen", envOrDefault("HOLOCRON_RAFT_LISTEN", ":9192"), "Raft RPC bind address (cluster mode)")
	peers := fs.String("peers", envOrDefault("HOLOCRON_PEERS", ""), "cluster membership as id=raft-addr=wire-addr,id=...,id=... (cluster mode)")
	bootstrap := fs.Bool("bootstrap", false, "bootstrap the cluster as the first node (cluster mode)")
	if err := fs.Parse(args); err != nil {
		return err
	}

	fmt.Printf("holocrond — stage %s\n", stage)

	var b *embed.Broker
	if *memory {
		fmt.Println("backend: in-memory (data lost on shutdown)")
		b = embed.NewMemory()
	} else {
		fmt.Printf("backend: disk at %s\n", *dataDir)
		opts := []embed.DiskOption{}
		if *retention > 0 {
			opts = append(opts, embed.WithRetention(*retention))
			fmt.Printf("retention: segments older than %s are deleted\n", *retention)
		}
		if *retentionBytes > 0 {
			opts = append(opts, embed.WithSizeRetention(*retentionBytes))
			fmt.Printf("retention: per-partition size capped at %d bytes\n", *retentionBytes)
		}
		if *clusterMode {
			peerList, err := parsePeers(*peers)
			if err != nil {
				return fmt.Errorf("--peers: %w", err)
			}
			if *nodeID == "" {
				return fmt.Errorf("--node-id is required in cluster mode")
			}
			opts = append(opts, embed.WithCluster(embed.ClusterConfig{
				NodeID:    *nodeID,
				BindAddr:  *raftBind,
				Peers:     peerList,
				Bootstrap: *bootstrap,
			}))
			fmt.Printf("cluster: node %s, raft %s, peers=%d, bootstrap=%v\n",
				*nodeID, *raftBind, len(peerList), *bootstrap)
		}
		var err error
		b, err = embed.NewDisk(*dataDir, opts...)
		if err != nil {
			return fmt.Errorf("open broker: %w", err)
		}
	}

	if topics := b.Topics(); len(topics) > 0 {
		fmt.Printf("recovered %d topic(s):\n", len(topics))
		for _, c := range topics {
			fmt.Printf("  %s (%d partitions)\n", c.Name, c.PartitionCount)
		}
	}

	if *listen != "" {
		addr, err := b.Listen(*listen)
		if err != nil {
			_ = b.Close()
			return fmt.Errorf("listen: %w", err)
		}
		fmt.Printf("listening on %s (wire v%d)\n", addr, 1)
	} else {
		fmt.Println("broker ready (network listener disabled)")
	}
	fmt.Println("press Ctrl-C to exit")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()
	fmt.Println("shutting down")

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = shutdown
	return b.Close()
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// parsePeers parses a "--peers" string of the form
// "id=raftaddr=wireaddr,id=raftaddr=wireaddr,..." into ClusterPeers.
// Empty input yields an empty slice.
func parsePeers(s string) ([]embed.ClusterPeer, error) {
	if s == "" {
		return nil, nil
	}
	var peers []embed.ClusterPeer
	for _, part := range strings.Split(s, ",") {
		fields := strings.Split(strings.TrimSpace(part), "=")
		if len(fields) != 3 {
			return nil, fmt.Errorf("expected id=raftaddr=wireaddr, got %q", part)
		}
		peers = append(peers, embed.ClusterPeer{
			ID:       fields[0],
			Addr:     fields[1],
			WireAddr: fields[2],
		})
	}
	return peers, nil
}
