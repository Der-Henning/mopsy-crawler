start:
	docker-compose -f docker-compose.builder.yml up --build
stop:
	docker-compose -f docker-compose.builder.yml down