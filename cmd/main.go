package main

import (
	"context"
	"imageProcessor/internal/components"
	"imageProcessor/internal/config"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/wb-go/wbf/zlog"
)

func main() {
	cfg, err := config.LoadConfig()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	// Инициализация глобального логгера zlog
	zlog.Init()
	logger := zlog.Logger

	appCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup

	components, err := components.InitComponents(appCtx, &wg, cfg, logger)
	if err != nil {
		logger.Error().Err(err).Msg("Could not init components")
		os.Exit(1)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := components.HttpServer.Run(appCtx); err != nil {
			logger.Error().Err(err).Msg("The http Server failed")
		}
		logger.Info().Msg("Http Server stopped")
	}()

	// Добавьте сюда wg.Add и запустите другие фоновые сервисы, если нужно
	// Например, если инициализация воркеров и Kafka consumer не внутри InitComponents,
	// запускайте тоже с wg.Add и wg.Done.

	quitChan := make(chan os.Signal, 1)
	signal.Notify(quitChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quitChan

	cancel()

	logger.Info().Str("signal", sig.String()).Msg("Captured signal, initiating shutdown")

	wg.Wait()

	logger.Info().Msg("Shutting down the services...")

	if shutdownErr := components.ShutdownAll(); shutdownErr != nil {
		logger.Error().Err(shutdownErr).Msg("could not properly stop components")
		os.Exit(1)
	}

	logger.Info().Msg("All components stopped. Giving system a moment to flush logs...")
	logger.Info().Msg("Gracefully shutting down the servers")
}
