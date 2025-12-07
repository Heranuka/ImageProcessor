ARGS=$(filter-out $@,$(MAKECMDGOALS))

# Kafka commands - все используют localhost:9092 для consistency
.PHONY: producer topics messages topic consumer-info reset-offsets

producer: 
	docker compose exec kafka-local kafka-console-producer.sh --bootstrap-server localhost:9092 --topic topic

topics:
	docker exec kafka-local kafka-topics.sh --bootstrap-server localhost:9092 --list

messages:
	docker exec kafka-local kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic topic --from-beginning

topic:
	docker exec kafka-local kafka-topics.sh --bootstrap-server localhost:9092 --create --topic topic --partitions 1 --replication-factor 1

consumer-info:
	docker exec kafka-local kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group my-consumer-group

reset-offsets:
	docker exec kafka-local kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group my-consumer-group --topic topic --reset-offsets --to-earliest --execute

# Database migrations
migrate-up:
	migrate -path ./internal/migration/schema -database 'postgres://postgres:1@0.0.0.0:5432/postgres?sslmode=disable' up

migrate-down:
	migrate -path ./internal/migration/schema -database 'postgres://postgres:1@0.0.0.0:5432/postgres?sslmode=disable' down

# Testing
testAll:
	go test -v -cover -coverpkg ./... ./...

# Docker management
dockerRun:
	docker build -t app . && docker compose up -d

dockerClear:
	docker compose down -v && docker rmi app
