package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/freetetra/server/internal/brew"
	"github.com/freetetra/server/internal/config"
	"github.com/freetetra/server/internal/service"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	if len(cfg.Blechelse.Talkgroups) == 0 {
		logger.Fatalf("blechelse init error: BLECHELSE_TALKGROUPS must list at least one TG")
	}
	plane := service.NewBrewModulePlane(cfg, logger, cfg.Blechelse.BrewISSI, cfg.Blechelse.Talkgroups)

	bridge, err := service.NewBlechelseBridge(cfg, logger, plane)
	if err != nil {
		logger.Fatalf("blechelse init error: %v", err)
	}

	plane.SetMessageHandlers(
		func(m *brew.CallControlMessage) { /* blechelse doesn't consume RX */ },
		func(m *brew.FrameMessage) { /* blechelse doesn't consume RX */ },
		nil,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		if err := plane.Run(ctx); err != nil {
			logger.Printf("brew module plane error: %v", err)
			cancel()
		}
	}()

	go func() {
		if err := bridge.Start(ctx); err != nil {
			logger.Printf("blechelse http server error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
}
