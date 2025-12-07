package render

import (
	"context"
	"net/http"

	"github.com/rs/zerolog"
	"github.com/wb-go/wbf/ginext"
)

type RendererHandler interface {
	Home(w http.ResponseWriter)
}
type Handler struct {
	logger zerolog.Logger
	render RendererHandler
}

func NewHandler(ctx context.Context, logger zerolog.Logger, render RendererHandler) *Handler {
	return &Handler{
		logger: logger,
		render: render,
	}
}

func (h *Handler) HomeHandler(c *ginext.Context) {
	h.render.Home(c.Writer)
}
