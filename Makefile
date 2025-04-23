SHELL := /bin/bash
PWD := $(shell pwd)

# Default target
default: docker-image

# Build all Docker images (adjust/add as needed)
docker-image:
	@echo "🛠️  Building Docker images..."
	docker build -f ./base_node/Dockerfile -t base_node:latest .
	docker build -f ./gateway/Dockerfile -t gateway:latest .
	docker build -f ./client/Dockerfile -t client:latest .
	docker build -f ./join_table/Dockerfile -t join_table:latest .
	docker build -f ./join_batch/credits/Dockerfile -t join_batch_credits:latest .
	docker build -f ./join_batch/ratings/Dockerfile -t join_batch_ratings:latest .
	docker build -f ./filter/cleanup/Dockerfile -t filter_cleanup:latest .
	docker build -f ./filter/year/Dockerfile -t filter_year:latest .
	docker build -f ./filter/production/Dockerfile -t filter_production:latest .
	docker build -f ./sentiment_analyzer/Dockerfile -t sentiment_analyzer:latest .
	docker build -f ./query/q1/Dockerfile -t query_q1:latest .
	docker build -f ./query/q2/Dockerfile -t query_q2:latest .
	docker build -f ./query/q3/Dockerfile -t query_q3:latest .
	docker build -f ./query/q4/Dockerfile -t query_q4:latest .
	docker build -f ./query/q5/Dockerfile -t query_q5:latest .
.PHONY: docker-image

# Start up the whole system with Docker Compose
docker-compose-up: docker-image
	@echo "🚀 Starting containers with docker-compose..."
	docker compose -f docker-compose.yaml up -d --build
.PHONY: docker-compose-up

docker-compose-up-nobuild:
	@echo "🚀 Starting containers with docker-compose (no build)..."
	docker compose -f docker-compose.yaml up -d --no-build
.PHONY: docker-compose-up-nobuild

# Shut down the whole system
docker-compose-down:
	@echo "🛑 Stopping and removing containers..."
	docker compose -f docker-compose.yaml stop -t 1
	docker compose -f docker-compose.yaml down
.PHONY: docker-compose-down

# Tail logs for all services
docker-compose-logs:
	@echo "📜 Showing logs (press Ctrl+C to exit)..."
	docker compose -f docker-compose.yaml logs -f
.PHONY: docker-compose-logs


# Logs dividided in multiple terminals
docker-compose-logs-to-file:
	@echo "🧹 Limpiando logs anteriores..."
	@rm -rf logs && mkdir -p logs

	@echo "📜 Guardando logs actuales de servicios en archivos dentro de ./logs/... "
	@services="gateway client join_table join_batch_credits join_batch_ratings filter_cleanup filter_year filter_production sentiment_analyzer query_q1 query_q2 query_q3 query_q4 query_q5"; \
	for svc in $$services; do \
		echo "📝 Guardando logs para $$svc en logs/$$svc.log"; \
		if docker compose -f docker-compose.yaml logs --no-log-prefix $$svc | tee logs/$$svc.log; then \
			echo "✅ Logs de $$svc guardados con éxito."; \
		else \
			echo "⚠️  Servicio $$svc no encontrado o no está corriendo, se saltea."; \
		fi; \
	done
.PHONY: docker-compose-logs-to-file

# Clean up previous logs
clean-logs:
	@echo "🧽 Borrando todos los logs en ./logs/..."
	@rm -rf logs
	@echo "✅ Logs eliminados."
.PHONY: clean-logs