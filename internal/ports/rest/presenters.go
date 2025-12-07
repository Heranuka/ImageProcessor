package rest

type createTaskRequest struct {
	ImageID   string `json:"image_id" binding:"required,uuid"`
	Operation string `json:"operation" binding:"required"`
}

type updateTaskStatusRequest struct {
	Status    string `json:"status" binding:"required"`
	ResultURL string `json:"result_url"`
}
