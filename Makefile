SHELL := /bin/bash
PWD := $(shell pwd)

# *************** MAKEFILES NUEVOS ***************

# Red común que usan ambos archivos
NETWORK=testing_net

# Nombres de los archivos de docker-compose
SYSTEM_FILE=docker-compose.system.yml
CLIENTS_FILE=docker-compose.clients.yml

# Construye las imágenes del sistema
build-system:
	docker build -f ./coordinator/Dockerfile -t coordinator:latest .
	docker build -f ./base_node/Dockerfile -t base_node:latest .
	docker build -f ./gateway/Dockerfile -t gateway:latest .
	docker build -f ./proxy/Dockerfile -t proxy:latest .
	docker build -f ./join/credits/Dockerfile -t join_credits:latest .
	docker build -f ./join/ratings/Dockerfile -t join_ratings:latest .
	docker build -f ./filter/cleanup/Dockerfile -t filter_cleanup:latest .
	docker build -f ./filter/year/Dockerfile -t filter_year:latest .
	docker build -f ./filter/production/Dockerfile -t filter_production:latest .
	docker build -f ./sentiment_analyzer/Dockerfile -t sentiment_analyzer:latest .
	docker build -f ./query/q1/Dockerfile -t query_q1:latest .
	docker build -f ./query/q2/Dockerfile -t query_q2:latest .
	docker build -f ./query/q3/Dockerfile -t query_q3:latest .
	docker build -f ./query/q4/Dockerfile -t query_q4:latest .
	docker build -f ./query/q5/Dockerfile -t query_q5:latest .

# Construye las imágenes de los clientes
build-clients:
	docker build -f ./client/Dockerfile -t client:latest .

# Levanta solo los servicios del sistema (gateway + server)
up-system:
	docker network inspect $(NETWORK) >/dev/null 2>&1 || docker network create $(NETWORK)
	docker compose -f $(SYSTEM_FILE) -p sistema up -d

# Levanta solo los servicios de los clientes
up-clients:
	docker compose -f $(CLIENTS_FILE) -p clientes up -d

# Muestra logs del sistema
logs-system:
	@echo "📜 Mostrando logs del sistema (Ctrl+C para salir)..."
	docker compose -f $(SYSTEM_FILE) -p sistema logs -f

# Muestra logs de los clientes
logs-clients:
	@echo "📜 Mostrando logs de los clientes (Ctrl+C para salir)..."
	docker compose -f $(CLIENTS_FILE) -p clientes logs -f

# Muestra logs combinados
logs-all:
	@echo "📜 Mostrando logs de todo el sistema (Ctrl+C para salir)..."
	docker compose -f $(SYSTEM_FILE) -p sistema logs -f
	docker compose -f $(CLIENTS_FILE) -p clientes logs -f

.PHONY: logs-system logs-clients logs-all

# Detiene los servicios del sistema y clientes
down:
	docker compose -f $(SYSTEM_FILE) -p sistema down
	docker compose -f $(CLIENTS_FILE) -p clientes down
	rm -rf ./join/ratings/storage/*
	rm -rf ./join/credits/storage/*
	rm -rf ./filter/cleanup/storage/*
	rm -rf ./filter/year/storage/*
	rm -rf ./filter/production/storage/*
	rm -rf ./sentiment_analyzer/storage/*
	rm -rf ./query/q1/storage/*
	rm -rf ./query/q2/storage/*
	rm -rf ./query/q3/storage/*
	rm -rf ./query/q4/storage/*
	rm -rf ./query/q5/storage/*

# Muestra los contenedores activos relacionados
ps:
	docker compose -f $(SYSTEM_FILE) -p sistema ps
	docker compose -f $(CLIENTS_FILE) -p clientes ps

# Borra los contenedores, redes y volúmenes asociados
clean:
	docker compose -f $(SYSTEM_FILE) -p sistema down -v
	docker compose -f $(CLIENTS_FILE) -p clientes down -v
	docker network rm $(NETWORK) 2>/dev/null || true

# Detiene solo los contenedores definidos en el sistema
docker-kill-system:
	@echo "🛑 Matando contenedores del sistema con SIGTERM..."
	@containers=$$(docker compose -f $(SYSTEM_FILE) -p sistema ps -q); \
	if [ -n "$$containers" ]; then \
		docker kill $$containers --signal=SIGTERM; \
		echo "✅ Contenedores del sistema detenidos."; \
	else \
		echo "⚠️  No hay contenedores del sistema en ejecución."; \
	fi
.PHONY: docker-kill-system

# Detiene solo los contenedores de los clientes
docker-kill-clients:
	@echo "🛑 Matando contenedores de clientes con SIGTERM..."
	@containers=$$(docker compose -f $(CLIENTS_FILE) -p clientes ps -q); \
	if [ -n "$$containers" ]; then \
		docker kill $$containers --signal=SIGTERM; \
		echo "✅ Contenedores de clientes detenidos."; \
	else \
		echo "⚠️  No hay contenedores de clientes en ejecución."; \
	fi
.PHONY: docker-kill-clients




# *************** MAKEFILES VIEJOS ***************

# Default target
default: docker-image

# Build all Docker images (adjust/add as needed)
docker-image:
	@echo "🛠️  Building Docker images..."
	docker build -f ./base_node/Dockerfile -t base_node:latest .
	docker build -f ./gateway/Dockerfile -t gateway:latest .
	docker build -f ./client/Dockerfile -t client:latest .
	docker build -f ./join/credits/Dockerfile -t join_credits:latest .
	docker build -f ./join/ratings/Dockerfile -t join_ratings:latest .
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
docker-compose-up:
	docker-image
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

# Kill all running containers
docker-kill:
	@echo "🛑 Deteniendo todos los contenedores..."
	@if [ -n "$$(docker ps -q)" ]; then \
		docker kill $$(docker ps -q) --signal=SIGTERM; \
		echo "✅ Todos los contenedores detenidos."; \
	else \
		echo "⚠️  No hay contenedores en ejecución."; \
	fi
.PHONY: docker-kill
