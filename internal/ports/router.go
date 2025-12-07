package ports

import (
	"context"
	"fmt"
	"imageProcessor/internal/config"
	"imageProcessor/internal/ports/render"
	"imageProcessor/internal/ports/rest"
	"net/http"
	"time"

	"github.com/gin-contrib/cors"
	"github.com/rs/zerolog"

	"github.com/wb-go/wbf/ginext"
)

type Server struct {
	logger zerolog.Logger
	server *ginext.Engine
	cfg    config.Config
}

func NewServer(ctx context.Context, cfg *config.Config, logger zerolog.Logger, h rest.ImageHandler, pros rest.ProcessingTaskStorage, rend render.RendererHandler) *Server {
	handler := rest.NewHandler(logger, h, pros)
	render := render.NewHandler(ctx, logger, rend)
	r := InitRouter(ctx, logger, handler, render)
	return &Server{
		logger: logger,
		server: r,
		cfg:    *cfg,
	}
}

func InitRouter(ctx context.Context, logger zerolog.Logger, h *rest.Handler, rend *render.Handler) *ginext.Engine {
	r := ginext.New("")
	r.RedirectTrailingSlash = false
	config := cors.DefaultConfig()
	config.AllowOrigins = []string{"http://localhost:8080"}
	config.AllowMethods = []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD"}
	config.AllowHeaders = []string{"Origin", "Content-Length", "Content-Type", "Authorization"}
	config.AllowCredentials = true
	r.Use(cors.New(config))
	r.Use(ginext.Logger())
	/* docsURL := ginSwagger.URL("http://localhost:8080/swagger/doc.json")
	r.GET("/swagger/*any", ginSwagger.WrapHandler(swaggerFiles.Handler, docsURL))
	*/
	r.GET("/", rend.HomeHandler)
	r.POST("/image/upload", h.UploadHandler)
	r.GET("/image/:id/status", h.GetImageStatusHandler)
	r.GET("/image/:id/file", h.GetImageFileHandler) // новый обработчик для отдачи файла

	r.DELETE("/image/:id", h.DeleteHandler)

	return r
}

func (s *Server) Run(ctx context.Context) error {
	errChan := make(chan error, 1)
	srv := &http.Server{
		Addr:    ":" + s.cfg.Http.Port,
		Handler: s.server,
	}
	go func() {
		s.logger.Info().Str("port", s.cfg.Http.Port).Msg("Starting listening")
		err := srv.ListenAndServe()

		if err != nil && err != http.ErrServerClosed {
			s.logger.Error().Err(err).Msg("Server failed to start")
			errChan <- fmt.Errorf("ListenAndServe error: %w", err)
		} else {
			errChan <- nil
		}
	}()

	select {
	case <-ctx.Done():
		s.logger.Info().Str("reason", ctx.Err().Error()).Msg("Shutting down the server...")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := srv.Shutdown(shutdownCtx); err != nil {
			s.logger.Error().Err(err).Msg("Server forced to shutdown")
			return err
		}
		s.logger.Info().Msg("Server stopped gracefully")
		return nil
	case err := <-errChan:
		if err != nil {
			s.logger.Error().Err(err).Msg("HttpServer failed")
			return err
		}
		s.logger.Info().Msg("HttpServer stopped gracefully (without external signal).")
		return nil
	}
}
