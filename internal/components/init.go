package components

import (
	"context"
	"errors"
	"fmt"
	"imageProcessor/internal/broker"
	"imageProcessor/internal/config"
	"imageProcessor/internal/ports"
	"imageProcessor/internal/service"
	"imageProcessor/internal/service/processors"
	"imageProcessor/internal/service/render"
	"imageProcessor/internal/storage"
	"log"
	"os"
	"sync"

	"github.com/rs/zerolog"
)

type Components struct {
	logger     zerolog.Logger
	HttpServer *ports.Server
	Postgres   *storage.Postgres
	WorkersWg  *sync.WaitGroup
	Broker     *broker.Kafka
}

func InitComponents(ctx context.Context, wg *sync.WaitGroup, cfg *config.Config, logger zerolog.Logger) (*Components, error) {
	// Создаем подключение к PostgreSQL
	postgres, err := storage.NewPostgres(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to init postgres: %w", err)
	}

	// Инициализируем Kafka брокер
	kafka := broker.NewKafka(logger, cfg)
	if kafka == nil {
		logger.Error().Msg("Components: Kafka initialization failed")
		return nil, fmt.Errorf("failed to init kafka")
	}

	// Текущая рабочая директория
	cwd, err := os.Getwd()
	if err != nil {
		log.Printf("failed to get current directory: %v", err)
		return nil, err
	}
	fmt.Println("Current working directory:", cwd)

	processor := processors.NewProcessor()
	render := render.NewRender(cwd+"/templates", logger)
	queue := service.NewChannelQueue(100)
	// Создаем файловое хранилище
	serviceFile := service.NewLocalFileStorage("baseDir")

	// Создаем сервис, передавая Kafka как и producer, и consumer
	serviceImage := service.NewService(kafka, postgres, serviceFile, processor, postgres, logger)

	// Запускаем воркеры обработки в фон

	go serviceImage.StartWorkers(ctx, wg, queue, 3)

	go func() {
		if err := serviceImage.ConsumeMessages(ctx, queue); err != nil && !errors.Is(err, context.Canceled) {
			logger.Error().Msgf("ConsumeMessages error: %v", err)
		}
	}()

	// Создаем HTTP сервер с необходимыми сервисами
	httpServer := ports.NewServer(ctx, cfg, logger, serviceImage, serviceImage, render)

	return &Components{
		logger:     logger,
		HttpServer: httpServer,
		Postgres:   postgres,
		WorkersWg:  wg,
		Broker:     kafka,
	}, nil
}

func (c *Components) ShutdownAll() error {
	c.logger.Info().Msg("ShutdownAll: starting...")
	c.Postgres.Close()
	c.logger.Info().Msg("ShutdownAll: finished")
	return nil
}
