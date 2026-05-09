// Stage 6 demo: a file-source connector reads `in.txt`, publishes each
// line to a holocron topic, and a file-sink connector consumes the topic
// and appends each line to `out.txt`. Both run in a single in-process
// worker against an embedded broker.
//
//	go run ./examples/connect
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/jedi-knights/holocron/broker/embed"
	"github.com/jedi-knights/holocron/connect"
	"github.com/jedi-knights/holocron/connect/file"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "holocron-connect-")
	if err != nil {
		return err
	}
	srcPath := filepath.Join(dir, "in.txt")
	dstPath := filepath.Join(dir, "out.txt")
	if err := os.WriteFile(srcPath, []byte("alpha\nbravo\ncharlie\ndelta\necho\n"), 0o644); err != nil {
		return err
	}

	b := embed.NewMemory()
	defer b.Close()
	if err := b.CreateTopic(embed.TopicSpec{Name: "events", PartitionCount: 1}); err != nil {
		return err
	}

	w, err := connect.NewWorker(b.Transport())
	if err != nil {
		return err
	}
	if err := w.AddSource(file.NewSource(file.SourceConfig{
		Name:  "file-source",
		Path:  srcPath,
		Topic: "events",
	}), 1); err != nil {
		return err
	}
	if err := w.AddSink(file.NewSink(file.SinkConfig{
		Name:  "file-sink",
		Topic: "events",
		Path:  dstPath,
	}), 1); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := w.Start(ctx); err != nil {
		return err
	}
	fmt.Printf("source: %s\nsink:   %s\nrunning until Ctrl-C or 3s timeout\n\n", srcPath, dstPath)

	select {
	case <-ctx.Done():
	case <-time.After(3 * time.Second):
	}

	if err := w.Stop(); err != nil {
		return err
	}

	out, err := os.ReadFile(dstPath)
	if err != nil {
		return err
	}
	fmt.Println("---- sink contents ----")
	fmt.Print(string(out))
	return nil
}
