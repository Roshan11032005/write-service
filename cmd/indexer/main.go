package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"write-service/internal/indexer"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	cfg, err := indexer.LoadBuilderConfig()
	if err != nil {
		log.Fatalf("[main] load config: %v", err)
	}

	builder, err := indexer.NewBuilder(cfg)
	if err != nil {
		log.Fatalf("[main] create builder: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigCh
		log.Printf("[main] received %v — shutting down", sig)
		cancel()
	}()

	log.Println("[main] starting index builder")
	if err := builder.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("[main] builder error: %v", err)
	}
	log.Println("[main] index builder stopped")
}
