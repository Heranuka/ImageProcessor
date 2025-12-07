package domain

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Image struct {
	ID        uuid.UUID // уникальный ID изображения
	Filename  string    // имя файла или путь в файловом хранилище
	FileType  string    // "jpg", "png", "gif" и др.
	Status    string    // статус обработки: "uploaded", "processing", "done", "error"
	CreatedAt time.Time
	UpdatedAt time.Time // время загрузки
}

// "uploaded", "processing", "done", "error"
type ProcessingTask struct {
	ID         uuid.UUID // уникальный ID задачи
	ImageID    uuid.UUID // связанный Image.ID
	Operation  string    // тип операции: "resize", "thumbnail", "watermark"
	Status     string    // статус задачи
	ResultURL  string    // путь к обработанному файлу
	CreatedAt  time.Time
	FinishedAt *time.Time
}

type ImageProcessingQueue interface {
	// Помещает задачу обработки изображения в очередь
	Enqueue(ctx context.Context, msg ProcessingTask) error

	// Получает задачу из очереди (блокирующий вызов или с таймаутом)
	ConsumeMessage(ctx context.Context) (ProcessingTask, error)

	// Подтверждает успешную обработку сообщения (если требуется)
	Commit(ctx context.Context, msg ProcessingTask) error
}

var ErrQueueEmpty = errors.New("queue is empty")
