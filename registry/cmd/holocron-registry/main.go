// Command holocron-registry is the holocron schema-registry daemon.
//
// It connects to a holocron broker via the network SDK, ensures the
// metadata topic exists, replays it to rebuild in-memory state, then
// serves the registry's HTTP API on the configured port.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jedi-knights/holocron/proto"
	holocronnet "github.com/jedi-knights/holocron/sdk/net"

	"github.com/jedi-knights/holocron/registry"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "holocron-registry:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("holocron-registry", flag.ContinueOnError)
	broker := fs.String("broker", envOrDefault("HOLOCRON_BROKER", "127.0.0.1:9092"), "broker address")
	listen := fs.String("listen", envOrDefault("HOLOCRON_REGISTRY_LISTEN", ":8081"), "HTTP listen address")
	topic := fs.String("topic", registry.DefaultTopic, "metadata topic name")
	if err := fs.Parse(args); err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	tr, err := holocronnet.Dial(*broker)
	if err != nil {
		return fmt.Errorf("dial %s: %w", *broker, err)
	}
	defer tr.Close()

	if err := tr.CreateTopic(ctx, *topic, 1); err != nil && !proto.IsStatus(err, proto.StatusInvalidRequest) {
		return fmt.Errorf("create %s: %w", *topic, err)
	}

	svc, err := registry.New(tr, registry.WithTopic(*topic))
	if err != nil {
		return err
	}
	defer svc.Close()
	if err := svc.Start(ctx); err != nil {
		return fmt.Errorf("start service: %w", err)
	}

	srv := &http.Server{
		Addr:              *listen,
		Handler:           registry.NewHandler(svc),
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 1)
	go func() {
		fmt.Printf("holocron-registry listening on %s, broker=%s, topic=%s\n", *listen, *broker, *topic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		fmt.Println("shutting down")
	case err := <-errCh:
		return err
	}

	shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return srv.Shutdown(shutdown)
}

func envOrDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
