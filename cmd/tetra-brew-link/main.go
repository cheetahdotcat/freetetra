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

	groups := service.LinkAttachTGs(cfg.Link.Pairs)
	if len(groups) == 0 {
		logger.Fatalf("link init error: LINK_PAIRS must define at least one pair (e.g. LINK_PAIRS=10:10000)")
	}
	plane := service.NewBrewModulePlane(cfg, logger, cfg.Link.BrewISSI, groups)

	bridge, err := service.NewLinkBridge(cfg, logger, plane)
	if err != nil {
		logger.Fatalf("link init error: %v", err)
	}

	plane.SetMessageHandlers(
		func(m *brew.CallControlMessage) {
			bridge.OnBrewCallControl(m)
		},
		func(m *brew.FrameMessage) {
			bridge.OnBrewFrame(m.Identifier, m.FrameType, m.Data)
		},
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

	if err := bridge.Start(ctx); err != nil {
		logger.Fatalf("link start error: %v", err)
	}
	defer bridge.Stop()

	<-ctx.Done()
}
