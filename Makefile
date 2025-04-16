SHELL := /bin/bash
PWD := $(shell pwd)

# Default target
default: docker-image

# Build all Docker images (adjust/add as needed)
docker-image:
	@echo "🛠️  Building Docker images..."
	docker build -f ./gateway/Dockerfile -t gateway:latest ./gateway
	docker build -f ./client/Dockerfile -t client:latest ./client
	docker build -f ./join_table/Dockerfile -t join_table:latest ./join_table
	docker build -f ./join_batch/credits/Dockerfile -t join_batch_credits:latest ./join_batch/credits
	docker build -f ./join_batch/ratings/Dockerfile -t join_batch_ratings:latest ./join_batch/ratings
	docker build -f ./filter/cleanup/Dockerfile -t filter_cleanup:latest ./filter/cleanup
	docker build -f ./filter/year/Dockerfile -t filter_year:latest ./filter/year
	docker build -f ./filter/production/Dockerfile -t filter_production:latest ./filter/production
	docker build -f ./sentiment_analyzer/Dockerfile -t sentiment_analyzer:latest ./sentiment_analyzer
	docker build -f ./query/q1/Dockerfile -t query_q1:latest ./query/q1
	docker build -f ./query/q2/Dockerfile -t query_q2:latest ./query/q2
	docker build -f ./query/q3/Dockerfile -t query_q3:latest ./query/q3
	docker build -f ./query/q4/Dockerfile -t query_q4:latest ./query/q4
	docker build -f ./query/q5/Dockerfile -t query_q5:latest ./query/q5
.PHONY: docker-image

# Start up the whole system with Docker Compose
docker-compose-up: docker-image
	@echo "🚀 Starting containers with docker-compose..."
	docker compose -f docker-compose-dev.yaml up -d --build
.PHONY: docker-compose-up

# Shut down the whole system
docker-compose-down:
	@echo "🛑 Stopping and removing containers..."
	docker compose -f docker-compose-dev.yaml stop -t 1
	docker compose -f docker-compose-dev.yaml down
.PHONY: docker-compose-down

# Tail logs for all services
docker-compose-logs:
	@echo "📜 Showing logs (press Ctrl+C to exit)..."
	docker compose -f docker-compose-dev.yaml logs -f
.PHONY: docker-compose-logs