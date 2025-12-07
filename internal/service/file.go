package service

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/uuid"
)

type FileStorage interface {
	PathForOriginal(ctx context.Context, imageID uuid.UUID, ext string) string
	PathForProcessed(ctx context.Context, imageID uuid.UUID, operation string, ext string) string
	SaveImage(ctx context.Context, filename, path, category string) error
	GetFileExtension(ctx context.Context, imageID uuid.UUID) (string, error)
	DeleteImage(ctx context.Context, filename, category string) error
}

type LocalFileStorage struct {
	baseDir string // корневая директория для хранения файлов
}

func NewLocalFileStorage(baseDir string) *LocalFileStorage {
	return &LocalFileStorage{baseDir: baseDir}
}

// Путь к оригинальному изображению: baseDir/originals/<imageID>.<ext>
func (fs *LocalFileStorage) PathForOriginal(ctx context.Context, imageID uuid.UUID, ext string) string {
	return filepath.Join(fs.baseDir, "originals", fmt.Sprintf("%s.%s", imageID.String(), ext))
}

// Путь к обработанному изображению: baseDir/processed/<operation>/<imageID>.<ext>
func (fs *LocalFileStorage) PathForProcessed(ctx context.Context, imageID uuid.UUID, operation string, ext string) string {
	return filepath.Join(fs.baseDir, "processed", operation, fmt.Sprintf("%s.%s", imageID.String(), ext))
}

func (fs *LocalFileStorage) SaveImage(ctx context.Context, filename, path, category string) error {
	dstDir := filepath.Join(fs.baseDir, category)
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return err
	}
	dstPath := filepath.Join(dstDir, filename)
	log.Printf("SaveImage: saving file %s to %s", filename, dstPath)

	input, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return os.WriteFile(dstPath, input, 0644)
}

// Удаление файла из указанной категории
func (fs *LocalFileStorage) DeleteImage(ctx context.Context, filename, category string) error {
	path := filepath.Join(fs.baseDir, category, filename)
	return os.Remove(path)
}

func (fs *LocalFileStorage) GetFileExtension(ctx context.Context, imageID uuid.UUID) (string, error) {
	// Получаем полный путь файла с расширением, например /storage/originals/<imageID>.jpg
	path := fs.PathForOriginal(ctx, imageID, "")
	ext := filepath.Ext(path) // вернет ".jpg"
	if ext == "" {
		return "", fmt.Errorf("file extension not found")
	}
	return strings.TrimPrefix(ext, "."), nil // вернет "jpg"
}
