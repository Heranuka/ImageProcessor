package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"imageProcessor/internal/domain"
	"imageProcessor/pkg/e"
	"time"

	"github.com/google/uuid"
)

type ImageStorage interface {
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

func (s *Postgres) CreateImageMetadata(ctx context.Context, image domain.Image) error {
	query := `INSERT INTO images (id, file_name, file_type, status, created_at, updated_at)
              VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := s.db.ExecContext(ctx, query, image.ID, image.Filename, image.FileType, image.Status, image.CreatedAt, image.UpdatedAt)
	if err != nil {
		return fmt.Errorf("CreateImageMetadata: failed to insert image metadata for id %s: %w", image.ID, err)
	}
	return nil
}

func (s *Postgres) UpdateStatus(ctx context.Context, imageID uuid.UUID, status string) error {
	query := `UPDATE images SET status = $1, updated_at = $2 WHERE id = $3`
	res, err := s.db.Master.ExecContext(ctx, query, status, time.Now(), imageID)
	if err != nil {
		return fmt.Errorf("UpdateStatus: failed to update status for image id %s: %w", imageID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("UpdateStatus: failed to get affected rows for image id %s: %w", imageID, err)
	}
	if rows == 0 {
		return fmt.Errorf("UpdateStatus: image not found with id %s", imageID)
	}
	return nil
}

func (s *Postgres) GetStatus(ctx context.Context, imageID uuid.UUID) (string, error) {
	var status string
	query := `SELECT status FROM images WHERE id = $1`
	err := s.db.QueryRowContext(ctx, query, imageID).Scan(&status)
	if err != nil {
		return "", fmt.Errorf("GetStatus: failed to get status for image id %s: %w", imageID, err)
	}
	return status, nil
}

func (s *Postgres) GetImageMetadata(ctx context.Context, imageID uuid.UUID) (domain.Image, error) {
	var image domain.Image
	query := `SELECT id, file_name, file_type, status, created_at, updated_at FROM images WHERE id = $1`
	err := s.db.QueryRowContext(ctx, query, imageID).Scan(&image.ID, &image.Filename, &image.FileType, &image.Status, &image.CreatedAt, &image.UpdatedAt)
	if err != nil {
		return domain.Image{}, fmt.Errorf("GetImageMetadata: failed to get metadata for image id %s: %w", imageID, err)
	}
	return image, nil
}

func (s *Postgres) DeleteImage(ctx context.Context, imageID uuid.UUID) error {
	query := `DELETE FROM images WHERE id = $1`
	res, err := s.db.ExecContext(ctx, query, imageID)
	if err != nil {
		return fmt.Errorf("DeleteImage: failed to delete image id %s: %w", imageID, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("DeleteImage: failed to get affected rows for image id %s: %w", imageID, err)
	}
	if rows == 0 {
		return fmt.Errorf("DeleteImage: image not found with id %s", imageID)
	}
	return nil
}

func (s *Postgres) CreateTask(ctx context.Context, task domain.ProcessingTask) error {
	query := `INSERT INTO processing_tasks (id, image_id, operation, status, result_url, created_at)
              VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := s.db.ExecContext(ctx, query, task.ID, task.ImageID, task.Operation, task.Status, task.ResultURL, task.CreatedAt)
	if err != nil {
		return fmt.Errorf("CreateTask: failed to create task id %s for image id %s: %w", task.ID, task.ImageID, err)
	}
	return nil
}

func (s *Postgres) UpdateTaskStatus(ctx context.Context, id uuid.UUID, status string, resultURL string) error {
	query := `UPDATE processing_tasks SET status = $1, result_url = $2, finished_at = $3 WHERE id = $4`
	res, err := s.db.ExecContext(ctx, query, status, resultURL, time.Now(), id)
	if err != nil {
		return fmt.Errorf("UpdateTaskStatus: failed to update task id %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("UpdateTaskStatus: failed to get affected rows for task id %s: %w", id, err)
	}
	if rows == 0 {
		return fmt.Errorf("UpdateTaskStatus: task not found with id %s", id)
	}
	return nil
}

func (s *Postgres) GetTaskByID(ctx context.Context, id uuid.UUID) (domain.ProcessingTask, error) {
	var task domain.ProcessingTask
	query := `SELECT id, image_id, operation, status, result_url, created_at, finished_at FROM processing_tasks WHERE image_id = $1`
	err := s.db.QueryRowContext(ctx, query, id).Scan(&task.ID, &task.ImageID, &task.Operation, &task.Status, &task.ResultURL, &task.CreatedAt, &task.FinishedAt)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ProcessingTask{}, e.ErrNotFound
		}
		return domain.ProcessingTask{}, fmt.Errorf("GetTaskByID: failed to get task id %s: %w", id, err)
	}
	return task, nil
}

func (s *Postgres) DeleteTask(ctx context.Context, id uuid.UUID) error {
	query := `DELETE FROM processing_tasks WHERE id = $1`
	res, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return e.ErrNotFound
		}
		return fmt.Errorf("DeleteTask: failed to delete task id %s: %w", id, err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("DeleteTask: failed to get affected rows for task id %s: %w", id, err)
	}
	if rows == 0 {
		return fmt.Errorf("DeleteTask: task not found with id %s", id)
	}
	return nil
}

func (s *Postgres) GetTaskByImageIDAndOperation(ctx context.Context, imageID uuid.UUID, operation string) (domain.ProcessingTask, error) {
	var task domain.ProcessingTask
	query := `SELECT id, image_id, operation, status, result_url, created_at, finished_at 
              FROM processing_tasks 
              WHERE image_id = $1 AND operation = $2 
              ORDER BY created_at DESC LIMIT 1`
	err := s.db.QueryRowContext(ctx, query, imageID, operation).Scan(
		&task.ID, &task.ImageID, &task.Operation, &task.Status, &task.ResultURL, &task.CreatedAt, &task.FinishedAt)
	if err != nil {
		return domain.ProcessingTask{}, fmt.Errorf("GetTaskByImageIDAndOperation: failed for image id %s operation %s: %w", imageID, operation, err)
	}
	return task, nil
}
