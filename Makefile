.PHONY: start stop restart logs validate run

start:
	docker-compose up -d

stop:
	docker-compose down

restart:
	docker-compose restart

logs:
	docker-compose logs -f krakend

logs-frontend:
	 docker-compose logs -f frontend

validate:
	docker run --rm -v "$${PWD}/krakend/config:/etc/krakend" krakend:latest check -d -c /etc/krakend/krakend.json

run:
	@echo "Starting all services..."
	@docker-compose up -d
	@echo "Services started. Frontend available at http://localhost:5173"
	@echo "API Gateway available at http://localhost:8080"