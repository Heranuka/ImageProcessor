package service

import (
	"context"
	"errors"
	"fmt"
	"imageProcessor/internal/domain"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const (
	workersNumber = 3
)

func (s *Service) StartWorkers(ctx context.Context, wg *sync.WaitGroup, queue *ChannelQueue, workerCount int) {
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			s.logger.Info().Msgf("Worker %d started", workerID)
			for {
				select {
				case <-ctx.Done():
					s.logger.Info().Msgf("Worker %d stopped due to context cancellation", workerID)
					return
				default:
					task, err := queue.ConsumeMessage(ctx)
					if err != nil {
						if errors.Is(err, context.Canceled) {
							s.logger.Info().Msgf("Worker %d: context canceled, exiting", workerID)
							return
						}
						fmt.Println("----------", task)
						s.logger.Error().Err(err).Msgf("Worker %d: failed to consume message", workerID)
						continue
					}
					if task.ID == uuid.Nil {
						s.logger.Warn().Msgf("Worker %d: received empty task, skipping", workerID)
						continue
					}

					s.logger.Info().Str("task_id", task.ID.String()).Msgf("Worker %d: start processing task", workerID)
					if err := s.handleProcessingTask(ctx, task); err != nil {
						s.logger.Error().
							Err(err).
							Str("task_id", task.ID.String()).
							Msgf("Worker %d: task processing failed", workerID)
						continue
					}
					s.logger.Info().Str("task_id", task.ID.String()).Msgf("Worker %d: task processed successfully", workerID)
				}
			}
		}(i)
	}
}

func (s *Service) handleProcessingTask(ctx context.Context, task domain.ProcessingTask) error {
	// Здесь ваша логика обработки одной задачи, например
	imageMeta, err := s.storage.GetImageMetadata(ctx, task.ImageID)
	if err != nil {
		return err
	}

	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(imageMeta.Filename)), ".")
	srcPath := s.fileStorage.PathForOriginal(ctx, task.ImageID, ext)
	dstPath := s.fileStorage.PathForProcessed(ctx, task.ImageID, task.Operation, ext)
	dstDir := filepath.Dir(dstPath)

	// Создаем директорию рекурсивно, если ее нет
	err = os.MkdirAll(dstDir, 0755)
	if err != nil {
		s.logger.Error().Err(err).Str("dir", dstDir).Msg("Failed to create directory for processed image")
		return fmt.Errorf("failed to create directory %s: %w", dstDir, err)
	}

	// Теперь можно безопасно создавать файл dstPath и сохранять обработанное изображение

	fmt.Println("----eeeee бади")
	err = s.processor.Process(task.ImageID, task.Operation, srcPath, dstPath)
	if err != nil {
		s.storage.UpdateStatus(ctx, task.ImageID, "error")
		return err
	}

	err = s.fileStorage.SaveImage(ctx, filepath.Base(dstPath), dstPath, "processed")
	if err != nil {
		return err
	}

	err = s.storage.UpdateStatus(ctx, task.ImageID, "finished")
	if err != nil {
		return err
	}

	return s.processingTaskStorage.UpdateTaskStatus(ctx, task.ID, "finished", dstPath)
}

/* func worker(ctx context.Context, s *Service, queue *ChannelQueue) {
	err := s.ConsumeMessages(ctx, queue)
	if err != nil && !errors.Is(err, context.Canceled) {
		s.logger.Error().Msgf("ConsumeMessages error: %v", err)
	}
	s.logger.Info().Msg("Worker stopped")
} */

/*
func handleMessage(ctx context.Context, s *Service, queue domain.ImageProcessingQueue) error {
	message, err := queue.ConsumeMessage(ctx)
	if err != nil {
		return fmt.Errorf("could not consume message from queue: %w", err)
	}

	imageID, err := uuid.Parse(message.ImageID.String())
	if err != nil {
		return fmt.Errorf("invalid image ID: %w", err)
	}
	operation := message.Operation

	ext, err := s.fileStorage.GetFileExtension(ctx, imageID)
	if err != nil {
		return fmt.Errorf("could not determine file extension: %w", err)
	}
	srcPath := s.fileStorage.PathForOriginal(ctx, imageID, ext)
	dstPath := s.fileStorage.PathForProcessed(ctx, imageID, operation, ext)

	// Вызов Process с параметрами по отдельности, а не с одной структурой
	err = s.processor.Process(imageID, operation, srcPath, dstPath)
	if err != nil {
		s.storage.UpdateStatus(ctx, imageID, "error")
		return fmt.Errorf("processing error: %w", err)
	}

	// перемещаем или сохраняем обработанный файл (зависит от реализации FileStorage)
	if err := s.fileStorage.SaveImage(ctx, filepath.Base(dstPath), dstPath, "processed"); err != nil {
		return fmt.Errorf("не удалось сохранить обработанное изображение: %w", err)
	}

	// обновляем статус
	if err := s.storage.UpdateStatus(ctx, imageID, "finished"); err != nil {
		return fmt.Errorf("не удалось обновить статус: %w", err)
	}

	// и удаляем временные файлы, если нужно
	os.Remove(srcPath)
	os.Remove(dstPath)

	return nil
}
*/
