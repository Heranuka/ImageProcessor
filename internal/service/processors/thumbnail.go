package processors

import (
	"fmt"
	"image"
	"image/jpeg"
	"image/png"
	"os"

	"github.com/nfnt/resize"
)

// thumbnail создаёт миниатюрное изображение с максимальной границей maxSize (width или height), сохраняя пропорции
func (p *Processor) Thumbnail(srcPath, dstPath string, maxSize uint, fileType string) error {
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

	thumb := resize.Thumbnail(maxSize, maxSize, img, resize.Lanczos3)

	outFile, err := os.Create(dstPath)
	if err != nil {
		return fmt.Errorf("create output file failed: %w", err)
	}
	defer outFile.Close()

	switch fileType {
	case "jpg", "jpeg":
		err = jpeg.Encode(outFile, thumb, nil)
	case "png":
		err = png.Encode(outFile, thumb)
	}
	if err != nil {
		return fmt.Errorf("encode output image failed: %w", err)
	}
	return nil
}
