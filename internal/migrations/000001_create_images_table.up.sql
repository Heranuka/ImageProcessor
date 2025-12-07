CREATE TABLE images (
    id UUID PRIMARY KEY,
    file_name TEXT NOT NULL,
    file_type VARCHAR(10) NULL,      
    status VARCHAR(20) NOT NULL,        
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE processing_tasks (
    id UUID PRIMARY KEY,
    image_id UUID NOT NULL REFERENCES images(id) ON DELETE CASCADE,
    operation VARCHAR(50) NOT NULL,       -- "resize", "thumbnail", "watermark"
    status VARCHAR(20) NOT NULL,          -- статус задачи
    result_url TEXT,                      -- путь к обработанному файлу
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMP WITH TIME ZONE
);
