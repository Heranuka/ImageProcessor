package processors

import (
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"
	"path/filepath"

	"github.com/google/uuid"
	"github.com/nfnt/resize"
)

// resized изменяет размер изображения до width x height, сохраняя пропорции (если height = 0)
func (p *Processor) Resized(srcPath, dstPath string, width, height uint, fileType string) error {
	file, err := os.Open(srcPath)
	if err != nil {
		return fmt.Errorf("open source image failed: %w", err)
	}
	defer file.Close()

	var img image.Image
	switch fileType {
	case "jpg", "jpeg":
		img, err = jpeg.Decode(file)
	case "png":
		img, err = png.Decode(file)
	default:
		return fmt.Errorf("unsupported image format: %s", fileType)
	}
	if err != nil {
		return fmt.Errorf("decode source image failed: %w", err)
	}

	resizedImg := resize.Resize(width, height, img, resize.Lanczos3)

	outFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create output file failed: %w", err)
	}
	defer outFile.Close()

	switch fileType {
	case "jpg", "jpeg":
		err = jpeg.Encode(outFile, resizedImg, nil)
	case "png":
		err = png.Encode(outFile, resizedImg)
	}
	if err != nil {
		return fmt.Errorf("encode output image failed: %w", err)
	}
	return nil
}

// helper функция формирования пути к файлу
func pathForProcessedImage(id uuid.UUID, operation, fileType string) string {
	filename := fmt.Sprintf("%s_%s.%s", id.String(), operation, fileType)
	return filepath.Join("storage", filename)
}
