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

	// Register every configured talkgroup up front so the brew server routes
	// RX to us and accepts TX on any of them without a runtime re-affiliation.
	tgs := service.SimplexPTTTalkgroups(cfg)
	if len(tgs) == 0 {
		logger.Fatalf("simplex-ptt has no talkgroups — set SIMPLEXPTT_TALKGROUPS")
	}

	plane := service.NewBrewModulePlane(cfg, logger, cfg.SimplexPTT.BrewISSI, tgs)

	bridge, err := service.NewSimplexPTTBridge(cfg, logger, plane)
	if err != nil {
		logger.Fatalf("simplex-ptt init error: %v", err)
	}

	// Wire the RX side: call-control drives the busy/talker indicator, traffic
	// frames feed the decoder that streams audio down to browsers.
	plane.SetMessageHandlers(
		func(m *brew.CallControlMessage) { bridge.OnBrewCallControl(m) },
		func(m *brew.FrameMessage) { bridge.OnBrewFrame(m.Identifier, m.FrameType, m.Data) },
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
			logger.Printf("simplex-ptt http server error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	bridge.Stop()
}
