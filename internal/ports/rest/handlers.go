package rest

import (
	"context"
	"imageProcessor/internal/domain"
	"mime/multipart"
	"net/http"
	"os"
	"path/filepath"

	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

type ImageHandler interface {
	UploadWithOperation(ctx context.Context, fileHeader *multipart.FileHeader, operation string) (uuid.UUID, error)
	GetStatus(ctx context.Context, imageID uuid.UUID) (string, error)
	DeleteImage(ctx context.Context, imageID uuid.UUID) error
	GetProcessedPath(imageID uuid.UUID, operation string) string
}

type ProcessingTaskStorage interface {
	CreateTask(ctx context.Context, task domain.ProcessingTask) error
	UpdateTaskStatus(ctx context.Context, id uuid.UUID, status string, resultURL string) error
	GetTaskByID(ctx context.Context, id uuid.UUID) (domain.ProcessingTask, error)
	DeleteTask(ctx context.Context, id uuid.UUID) error
}
type Handler struct {
	logger                zerolog.Logger
	svc                   ImageHandler
	processingTaskStorage ProcessingTaskStorage
}

func NewHandler(logger zerolog.Logger, imageHandler ImageHandler, processingTaskStorage ProcessingTaskStorage) *Handler {
	return &Handler{
		logger:                logger,
		svc:                   imageHandler,
		processingTaskStorage: processingTaskStorage,
	}
}
func (h *Handler) UploadHandler(c *gin.Context) {
	fileHeader, err := c.FormFile("file")
	if err != nil {
		h.logger.Error().Err(err).Msg("UploadHandler: file is required")
		c.JSON(http.StatusBadRequest, gin.H{"error": "file is required"})
		return
	}
	h.logger.Info().Msgf("UploadHandler: received file %s", fileHeader.Filename)

	operation := c.PostForm("operation")
	if operation == "" {
		operation = "resize"
		h.logger.Info().Msg("UploadHandler: operation not specified, using default ‘resize’")
	} else {
		h.logger.Info().Msgf("UploadHandler: operation %s", operation)
	}

	id, err := h.svc.UploadWithOperation(c.Request.Context(), fileHeader, operation)
	if err != nil {
		h.logger.Error().Err(err).Msg("UploadHandler: failed to upload image")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	h.logger.Info().Msgf("UploadHandler: file uploaded successfully with ID %s", id.String())
	c.JSON(http.StatusOK, gin.H{
		"id":     id.String(),
		"status": "uploaded",
	})
}

func (h *Handler) GetImageStatusHandler(c *gin.Context) {
	idStr := c.Param("id")
	imageID, err := uuid.Parse(idStr)
	if err != nil {
		h.logger.Error().Err(err).Str("id", idStr).Msg("GetImageStatusHandler: invalid image id")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid image id"})
		return
	}

	status, err := h.svc.GetStatus(c.Request.Context(), imageID)
	if err != nil {
		h.logger.Error().Err(err).Str("image_id", imageID.String()).Msg("GetImageStatusHandler: failed to get status")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to get status"})
		return
	}
	h.logger.Info().Msgf("GetImageStatusHandler: status for image %s is %s", imageID.String(), status)

	c.JSON(http.StatusOK, gin.H{
		"status": status,
	})
}

func (h *Handler) GetImageFileHandler(c *gin.Context) {
	imageID := c.Param("id") // например, b2cb90b1-46ac-4eaf-95be-b7392b14b748
	filePath := filepath.Join("baseDir", "processed", imageID+".png")

	if _, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			c.AbortWithStatus(404)
			return
		}
		c.AbortWithStatus(500)
		return
	}

	c.File(filePath)
}

func (h *Handler) DeleteHandler(c *gin.Context) {
	idStr := c.Param("id")
	imageID, err := uuid.Parse(idStr)
	if err != nil {
		h.logger.Error().Err(err).Str("id", idStr).Msg("DeleteHandler: invalid image id")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid id"})
		return
	}

	err = h.svc.DeleteImage(c.Request.Context(), imageID)
	if err != nil {
		h.logger.Error().Err(err).Str("image_id", imageID.String()).Msg("DeleteHandler: failed to delete image")
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	h.logger.Info().Msgf("DeleteHandler: image %s deleted successfully", imageID.String())

	c.Status(http.StatusNoContent)
}

func (h *Handler) CreateTask(c *gin.Context) {
	var req createTaskRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error().Err(err).Msg("CreateTask: invalid request")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	imageID, err := uuid.Parse(req.ImageID)
	if err != nil {
		h.logger.Error().Err(err).Str("image_id", req.ImageID).Msg("CreateTask: invalid image id")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid image id"})
		return
	}

	task := domain.ProcessingTask{
		ImageID:   imageID,
		Operation: req.Operation,
	}
	ctx := c.Request.Context()
	err = h.processingTaskStorage.CreateTask(ctx, task)
	if err != nil {
		h.logger.Error().Err(err).Msg("CreateTask: failed to create task")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to create task"})
		return
	}
	h.logger.Info().Msgf("CreateTask: task created with id %s for image %s", task.ID.String(), imageID.String())
	c.JSON(http.StatusCreated, gin.H{"id": task.ID.String()})
}

func (h *Handler) GetTask(c *gin.Context) {
	taskIDStr := c.Param("id")
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		h.logger.Error().Err(err).Str("id", taskIDStr).Msg("GetTask: invalid task id")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
		return
	}
	ctx := c.Request.Context()
	task, err := h.processingTaskStorage.GetTaskByID(ctx, taskID)
	if err != nil {
		h.logger.Error().Err(err).Str("task_id", taskID.String()).Msg("GetTask: task not found")
		c.JSON(http.StatusNotFound, gin.H{"error": "task not found"})
		return
	}
	h.logger.Info().Msgf("GetTask: fetched task %s", task.ID.String())
	c.JSON(http.StatusOK, task)
}

func (h *Handler) UpdateTaskStatus(c *gin.Context) {
	taskIDStr := c.Param("id")
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		h.logger.Error().Err(err).Str("id", taskIDStr).Msg("UpdateTaskStatus: invalid task id")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
		return
	}
	var req updateTaskStatusRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		h.logger.Error().Err(err).Msg("UpdateTaskStatus: invalid request body")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid request"})
		return
	}
	ctx := c.Request.Context()
	err = h.processingTaskStorage.UpdateTaskStatus(ctx, taskID, req.Status, req.ResultURL)
	if err != nil {
		h.logger.Error().Err(err).Str("task_id", taskID.String()).Msg("UpdateTaskStatus: failed to update task status")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to update task status"})
		return
	}
	h.logger.Info().Msgf("UpdateTaskStatus: task %s status updated to %s", taskID.String(), req.Status)
	c.JSON(http.StatusOK, gin.H{"status": "updated"})
}

func (h *Handler) DeleteTask(c *gin.Context) {
	taskIDStr := c.Param("id")
	taskID, err := uuid.Parse(taskIDStr)
	if err != nil {
		h.logger.Error().Err(err).Str("id", taskIDStr).Msg("DeleteTask: invalid task id")
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid task id"})
		return
	}
	ctx := c.Request.Context()
	err = h.processingTaskStorage.DeleteTask(ctx, taskID)
	if err != nil {
		h.logger.Error().Err(err).Str("task_id", taskID.String()).Msg("DeleteTask: failed to delete task")
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to delete task"})
		return
	}
	h.logger.Info().Msgf("DeleteTask: task %s deleted successfully", taskID.String())
	c.JSON(http.StatusOK, gin.H{"status": "deleted"})
}
