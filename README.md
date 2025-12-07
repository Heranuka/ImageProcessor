ImageProcessor

🚀 Установка и запуск
 
 Запустите проект:
    
    1) make dockerRun - ВСЕ УСТАНОВИТСЯ
    2) make topic
    нужно обязательно создать топик

🧪 Тестирование

Чтобы затестировать код и узнать покрытие введите команду:

    make testAll

💻 Технический стек

    Язык программирования: Go 1.25
    Web-фреймворк: Ginext
    База данных: PostgreSQL
    Миграции: Migrate
    Логгирование: zerolog
    Документация API: Swagger
    Кэш: Redis 
    Контейнеризация: Docker, Docker Compose

HTTP API

r.GET("/", rend.HomeHandler)
	r.POST("/image/upload", h.UploadHandler)
	r.GET("/image/:id/status", h.GetImageStatusHandler)
	r.GET("/image/:id/file", h.GetImageFileHandler) 

	r.DELETE("/image/:id", h.DeleteHandler)






