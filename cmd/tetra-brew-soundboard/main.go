package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/freetetra/server/internal/config"
	"github.com/freetetra/server/internal/service"
)

func main() {
	cfg, err := config.LoadFromEnv()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	logger := log.New(os.Stdout, "", log.LstdFlags|log.Lmicroseconds)

	// Load manifest up front so the BrewModulePlane registers all of the
	// talkgroups the soundboard will ever transmit on. A press for a button
	// whose TG wasn't pre-registered would be rejected by the brew server.
	manifest, err := service.LoadSoundboardManifest(cfg)
	if err != nil {
		logger.Fatalf("soundboard manifest error: %v", err)
	}
	tgs := service.SoundboardTalkgroups(manifest)
	if len(tgs) == 0 {
		logger.Fatalf("soundboard manifest has no talkgroups — add at least one button with a non-zero tg")
	}

	plane := service.NewBrewModulePlane(cfg, logger, cfg.Soundboard.BrewISSI, tgs)

	bridge, err := service.NewSoundboardBridge(cfg, logger, plane)
	if err != nil {
		logger.Fatalf("soundboard init error: %v", err)
	}

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
			logger.Printf("soundboard http server error: %v", err)
			cancel()
		}
	}()

	<-ctx.Done()
	bridge.Stop()
}
