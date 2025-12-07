package service

import (
	"context"
	"errors"
	"fmt"
	"imageProcessor/internal/domain"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	kafka_broker "imageProcessor/internal/broker"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type Storage interface {
	CreateImageMetadata(ctx context.Context, image domain.Image) error
	UpdateStatus(ctx context.Context, imageID uuid.UUID, status string) error
	GetStatus(ctx context.Context, imageID uuid.UUID) (string, error)
	GetImageMetadata(ctx context.Context, imageID uuid.UUID) (domain.Image, error)
	DeleteImage(ctx context.Context, imageID uuid.UUID) error
}

type ProcessingTaskStorage interface {
	CreateTask(ctx context.Context, task domain.ProcessingTask) error
	UpdateTaskStatus(ctx context.Context, id uuid.UUID, status string, resultURL string) error
	GetTaskByID(ctx context.Context, id uuid.UUID) (domain.ProcessingTask, error)
	DeleteTask(ctx context.Context, id uuid.UUID) error
	GetTaskByImageIDAndOperation(ctx context.Context, imageID uuid.UUID, operation string) (domain.ProcessingTask, error)
}

type ImageProcessor interface {
	Process(imageID uuid.UUID, operation, srcPath, dstPath string) error
	Resized(srcPath, dstPath string, width, height uint, fileType string) error
	Thumbnail(srcPath, dstPath string, maxSize uint, fileType string) error
	Watermark(srcPath, watermarkPath, dstPath string, posX, posY int, alpha float64, fileType string) error
}

type Service struct {
	logger                zerolog.Logger
	kafka                 *kafka_broker.Kafka
	storage               Storage
	processor             ImageProcessor
	fileStorage           FileStorage
	processingTaskStorage ProcessingTaskStorage
}

func NewService(
	kafka *kafka_broker.Kafka,
	storage Storage,
	fileStorage FileStorage,
	processor ImageProcessor,
	processingTaskStorage ProcessingTaskStorage,
	logger zerolog.Logger,
) *Service {
	return &Service{
		kafka:                 kafka,
		storage:               storage,
		fileStorage:           fileStorage,
		processor:             processor,
		processingTaskStorage: processingTaskStorage,
		logger:                logger,
	}
}

func (s *Service) UploadWithOperation(ctx context.Context, fileHeader *multipart.FileHeader, operation string) (uuid.UUID, error) {
	id := uuid.New()
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(fileHeader.Filename)), ".")

	srcPath := s.fileStorage.PathForOriginal(ctx, id, ext)

	srcFile, err := fileHeader.Open()
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to open uploaded file")
		return id, err
	}
	defer srcFile.Close()

	if err := os.MkdirAll(filepath.Dir(srcPath), 0755); err != nil {
		s.logger.Error().Err(err).Msg("failed to create directories for file")
		return id, err
	}

	dstFile, err := os.Create(srcPath)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to create destination file")
		return id, err
	}
	defer dstFile.Close()

	if _, err := io.Copy(dstFile, srcFile); err != nil {
		s.logger.Error().Err(err).Msg("failed to copy file content")
		return id, err
	}

	fileType, err := DetectFileType(srcPath)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to detect file type")
		return id, err
	}

	now := time.Now()
	image := domain.Image{
		ID:        id,
		Filename:  fileHeader.Filename,
		FileType:  fileType,
		Status:    "new",
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := s.storage.CreateImageMetadata(ctx, image); err != nil {
		s.logger.Error().Err(err).Msg("failed to create image metadata")
		return id, err
	}

	// Создаем задачу обработки со статусом "new"
	task := domain.ProcessingTask{
		ID:        uuid.New(),
		ImageID:   image.ID,
		Operation: operation,
		Status:    "new",
		CreatedAt: now,
	}
	if err := s.CreateTask(ctx, task); err != nil {
		s.logger.Error().Err(err).Msg("failed to create processing task")
		return uuid.Nil, err
	}

	// Обновляем статус image и task на "uploaded"
	if err := s.storage.UpdateStatus(ctx, id, "uploaded"); err != nil {
		s.logger.Error().Err(err).Msg("failed to update image status")
		return id, err
	}
	if err := s.processingTaskStorage.UpdateTaskStatus(ctx, task.ID, "uploaded", srcPath); err != nil {
		s.logger.Error().Err(err).Msg("failed to update task status")
		return id, err
	}

	// Обновляем поле статуса в объекте для Kafka
	image.Status = "uploaded"

	// Публикуем сообщение в Kafka уже с новым статусом
	if err := s.kafka.Produce(ctx, image); err != nil {
		s.logger.Error().Err(err).Msg("failed to send kafka message")
	}

	return id, nil
}

func (s *Service) ConsumeMessages(ctx context.Context, queue *ChannelQueue) error {
	for {
		image, err := s.kafka.Consume(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				s.logger.Info().Msg("ConsumeMessages: context cancelled, stopping consumer")
				return nil
			}
			s.logger.Error().Err(err).Msg("ConsumeMessages: kafka consume error")
			continue
		}
		s.logger.Info().Str("image_id", image.ID.String()).Str("status", image.Status).Msg("ConsumeMessages: message received from kafka")

		fmt.Println("leeeeeeeeee", image.ID.String())
		operation, err := s.GetOperationForImage(ctx, image.ID)
		if err != nil {
			s.logger.Error().
				Err(err).
				Str("image_id", image.ID.String()).
				Msg("ConsumeMessages: failed to get operation for image")
			continue
		}

		// Поиск задачи по операции "resize", а не по статусу uploaded
		task, err := s.processingTaskStorage.GetTaskByImageIDAndOperation(ctx, image.ID, operation)
		if err != nil {
			s.logger.Error().
				Err(err).
				Str("image_id", image.ID.String()).
				Str("status", image.Status).
				Msg("ConsumeMessages: task not found for image and operation 'resize'")
			continue
		}
		s.logger.Info().Str("task_id", task.ID.String()).Str("operation", task.Operation).Msg("ConsumeMessages: task fetched from DB")

		if err := queue.Enqueue(ctx, task); err != nil {
			s.logger.Error().
				Err(err).
				Str("task_id", task.ID.String()).
				Msg("ConsumeMessages: failed to enqueue task")
			continue
		}
		s.logger.Info().Str("task_id", task.ID.String()).Msg("ConsumeMessages: task enqueued successfully")
	}
}

func (s *Service) handleMessage(ctx context.Context, image *domain.Image) error {
	imageMeta, err := s.storage.GetImageMetadata(ctx, image.ID)
	if err != nil {
		s.logger.Error().
			Err(err).
			Str("image_id", image.ID.String()).
			Msg("handleMessage: failed to get image metadata")
		s.storage.UpdateStatus(ctx, image.ID, "error")
		return err
	}

	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(imageMeta.Filename)), ".")
	srcPath := s.fileStorage.PathForOriginal(ctx, image.ID, ext)
	fmt.Println("le", image.ID.String())
	task, err := s.processingTaskStorage.GetTaskByImageIDAndOperation(ctx, image.ID, image.Status)
	if err != nil {
		s.logger.Warn().
			Err(err).
			Str("image_id", image.ID.String()).
			Msg("handleMessage: processing task not found for image and status")
		return err
	}
	operation := task.Operation

	dstPath := s.fileStorage.PathForProcessed(ctx, image.ID, operation, ext)

	s.logger.Info().
		Str("image_id", image.ID.String()).
		Str("operation", operation).
		Msg("handleMessage: processing image")

	if err := s.processor.Process(image.ID, operation, srcPath, dstPath); err != nil {
		s.logger.Error().
			Err(err).
			Str("image_id", image.ID.String()).
			Str("operation", operation).
			Msg("handleMessage: processing failed")
		s.storage.UpdateStatus(ctx, image.ID, "error")
		return err
	}

	if err := s.fileStorage.SaveImage(ctx, filepath.Base(dstPath), dstPath, "processed"); err != nil {
		s.logger.Error().
			Err(err).
			Str("image_id", image.ID.String()).
			Msg("handleMessage: failed to save processed image")
		return fmt.Errorf("failed to save processed image: %w", err)
	}

	if err := s.storage.UpdateStatus(ctx, image.ID, "finished"); err != nil {
		s.logger.Error().
			Err(err).
			Str("image_id", image.ID.String()).
			Msg("handleMessage: failed to update image status")
		return fmt.Errorf("failed to update image status: %w", err)
	}

	if err := s.processingTaskStorage.UpdateTaskStatus(ctx, task.ID, "finished", dstPath); err != nil {
		s.logger.Error().
			Err(err).
			Msg("handleMessage: failed to update task status")
	}

	os.Remove(srcPath)
	os.Remove(dstPath)

	s.logger.Info().
		Str("image_id", image.ID.String()).
		Msg("handleMessage: image processed successfully")

	return nil
}

func DetectFileType(filePath string) (string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	buffer := make([]byte, 512)
	n, err := file.Read(buffer)
	if err != nil {
		return "", err
	}
	if n == 0 {
		return "", errors.New("file is empty")
	}

	contentType := http.DetectContentType(buffer[:n])
	sLogger := zerolog.New(os.Stdout) // Для логгирования детекта можно использовать логгер сервиса, если хотите
	sLogger.Debug().Msgf("Detected content type: %s", contentType)

	switch contentType {
	case "image/jpeg":
		return "jpg", nil
	case "image/png":
		return "png", nil
	case "image/gif":
		return "gif", nil
	case "image/webp":
		return "webp", nil
	case "image/tiff":
		return "tiff", nil
	default:
		ext := filepath.Ext(filePath)
		if len(ext) > 0 {
			return ext[1:], nil
		}
		return "", fmt.Errorf("unknown file type, content-type: %s", contentType)
	}
}

func (s *Service) GetStatus(ctx context.Context, imageID uuid.UUID) (string, error) {
	return s.storage.GetStatus(ctx, imageID)
}
func (s *Service) GetImageMetadata(ctx context.Context, imageID uuid.UUID) (domain.Image, error) {
	return s.storage.GetImageMetadata(ctx, imageID)
}
func (s *Service) DeleteImage(ctx context.Context, imageID uuid.UUID) error {
	return s.storage.DeleteImage(ctx, imageID)
}

func (s *Service) GetTaskByImageIDAndOperation(ctx context.Context, imageID uuid.UUID, operation string) (domain.ProcessingTask, error) {
	return s.processingTaskStorage.GetTaskByImageIDAndOperation(ctx, imageID, operation)
}

func (s *Service) CreateTask(ctx context.Context, task domain.ProcessingTask) error {
	return s.processingTaskStorage.CreateTask(ctx, task)
}

func (s *Service) DeleteTask(ctx context.Context, id uuid.UUID) error {
	return s.processingTaskStorage.DeleteTask(ctx, id)
}

func (s *Service) GetTaskByID(ctx context.Context, id uuid.UUID) (domain.ProcessingTask, error) {
	return s.processingTaskStorage.GetTaskByID(ctx, id)
}

func (s *Service) UpdateTaskStatus(ctx context.Context, id uuid.UUID, status string, resultURL string) error {
	return s.processingTaskStorage.UpdateTaskStatus(ctx, id, status, resultURL)
}

func (s *Service) GetProcessedPath(imageID uuid.UUID, operation string) string {
	imageMeta, err := s.storage.GetImageMetadata(context.Background(), imageID)
	if err != nil {
		s.logger.Error().Err(err).Str("image_id", imageID.String()).Msg("failed to get image metadata")
		return ""
	}
	ext := strings.TrimPrefix(strings.ToLower(filepath.Ext(imageMeta.Filename)), ".")
	return s.fileStorage.PathForProcessed(context.Background(), imageID, operation, ext)
}

func (s *Service) GetOperationForImage(ctx context.Context, imageID uuid.UUID) (string, error) {
	// Попытка получить операцию из базы по imageID
	task, err := s.GetTaskByID(ctx, imageID)
	fmt.Println("-------------", task)
	if err != nil {
		return "", err
	}

	// Возвращаем операцию задачи (например, "resize", "thumbnail" и т.п.)
	return task.Operation, nil
}
