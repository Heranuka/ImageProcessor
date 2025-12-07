package processors

import (
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type ImageProcessor interface {
	Process(imageID uuid.UUID, operation, srcPath, dstPath string) error
	Resized(srcPath, dstPath string, width, height uint, fileType string) error
	Thumbnail(srcPath, dstPath string, maxSize uint, fileType string) error
	Watermark(srcPath, watermarkPath, dstPath string, posX, posY int, alpha float64, fileType string) error
}

type Processor struct{}

func NewProcessor() *Processor {
	return &Processor{}
}
func (p *Processor) Process(imageID uuid.UUID, operation, srcPath, dstPath string) error {
	fileType, err := DetectFileType(srcPath)
	if err != nil {
		return fmt.Errorf("detect file type failed: %w", err)
	}

	switch operation {
	case "resize":
		return p.Resized(srcPath, dstPath, 800, 0, fileType)
	case "thumbnail":
		return p.Thumbnail(srcPath, dstPath, 150, fileType)
	case "watermark":
		GenerateWatermark("watermark.png", "Chama or no chama?")
		return p.Watermark(srcPath, "watermark.png", dstPath, 10, 10, 0.5, fileType)
	default:
		return fmt.Errorf("unknown task action: %s", operation)
	}
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
