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

	// The SIP gateway is an individual subscriber, not a talkgroup member: it
	// registers as its GatewayISSI and handles private calls to/from that ISSI,
	// bridging them to a SIP/RTP peer. No group affiliation is needed.
	plane := service.NewBrewModulePlane(cfg, logger, cfg.SIP.GatewayISSI, nil)

	bridge, err := service.NewSIPBridge(cfg, logger, plane)
	if err != nil {
		logger.Fatalf("sip init error: %v", err)
	}

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
			logger.Printf("sip bridge error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	bridge.Stop()
}
