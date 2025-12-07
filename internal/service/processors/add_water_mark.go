package processors

import (
	"image"
	"image/color"
	"image/draw"
	"image/png"
	"os"

	"github.com/disintegration/imaging"
	"golang.org/x/image/font"
	"golang.org/x/image/font/basicfont"
	"golang.org/x/image/math/fixed"
)

func (p *Processor) Watermark(srcPath, watermarkPath, dstPath string, posX, posY int, alpha float64, fileType string) error {
	srcImg, err := imaging.Open(srcPath)
	if err != nil {
		return err
	}
	wmImg, err := imaging.Open(watermarkPath) // водяной знак в png с альфа-каналом
	if err != nil {
		return err
	}
	// Накладываем водяной знак с прозрачностью alpha по позиции posX,posY
	result := imaging.Overlay(srcImg, wmImg, image.Pt(posX, posY), alpha)

	err = imaging.Save(result, dstPath)
	if err != nil {
		return err
	}
	return nil
}

// GenerateWatermark создает простой PNG с текстом водяного знака
func GenerateWatermark(filePath, text string) error {
	width, height := 200, 50
	rgba := image.NewRGBA(image.Rect(0, 0, width, height))

	// Прозрачный фон
	draw.Draw(rgba, rgba.Bounds(), &image.Uniform{color.Transparent}, image.Point{}, draw.Src)

	// Черный цвет текста
	col := color.RGBA{0, 0, 0, 255}

	addLabel(rgba, 10, 30, text, col)

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := png.Encode(f, rgba); err != nil {
		return err
	}

	return nil
}

func addLabel(img *image.RGBA, x, y int, label string, col color.Color) {
	point := fixed.Point26_6{fixed.Int26_6(x * 64), fixed.Int26_6(y * 64)}
	drawer := &font.Drawer{
		Dst:  img,
		Src:  image.NewUniform(col),
		Face: basicfont.Face7x13,
		Dot:  point,
	}
	drawer.DrawString(label)
}
