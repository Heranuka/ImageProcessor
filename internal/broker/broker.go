package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"imageProcessor/internal/config"
	"imageProcessor/internal/domain"
	"time"

	"github.com/rs/zerolog"
	"github.com/wb-go/wbf/kafka"
	"github.com/wb-go/wbf/retry"
)

type Kafka struct {
	logger   zerolog.Logger
	Producer *kafka.Producer
	Consumer *kafka.Consumer
}

func NewKafka(logger zerolog.Logger, cfg *config.Config) *Kafka {
	producer := kafka.NewProducer(cfg.Kafka.BrokerList, cfg.Kafka.Topic)
	consumer := kafka.NewConsumer(cfg.Kafka.BrokerList, cfg.Kafka.Topic, cfg.Kafka.ConsumerGroup)

	return &Kafka{
		Producer: producer,
		Consumer: consumer,
	}
}

func (k *Kafka) Produce(ctx context.Context, data domain.Image) error {
	strategy := retry.Strategy{
		Attempts: 3,
		Delay:    time.Second,
		Backoff:  1,
	}
	payload, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("marshal kafka payload failed: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			if err := k.Producer.Close(); err != nil {
				k.logger.Error().Err(err).Msg("producer close failed")
			}
			return ctx.Err()
		default:
			if err := k.Producer.SendWithRetry(ctx, strategy, nil, payload); err != nil {
				k.logger.Error().Err(err).Msg("kafka produce failed")
				return err
			}
			k.logger.Info().Msgf("produced message with id: %s", data.ID.String())
			return nil
		}
	}
}

func (k *Kafka) Consume(ctx context.Context) (*domain.Image, error) {
	strategy := retry.Strategy{
		Attempts: 3,
		Delay:    time.Second,
		Backoff:  1,
	}
	var image domain.Image

	for {
		select {
		case <-ctx.Done():
			if err := k.Consumer.Close(); err != nil {
				k.logger.Error().Err(err).Msg("consumer close failed")
			}
			return nil, ctx.Err()
		default:
			msg, err := k.Consumer.FetchWithRetry(ctx, strategy)
			if err != nil {
				k.logger.Error().Err(err).Msg("kafka fetch failed")
				return nil, err
			}
			if err := json.Unmarshal(msg.Value, &image); err != nil {
				k.logger.Error().Err(err).Msg("unmarshal kafka message failed")
				continue // пропускаем проблемное сообщение
			}
			if err := k.Consumer.Commit(ctx, msg); err != nil {
				k.logger.Error().Err(err).Msg("kafka commit failed")
				continue
			}
			k.logger.Info().Msgf("consumed message with id: %s", image.ID)
			return &image, nil
		}
	}
}
