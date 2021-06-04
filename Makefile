start:
	docker-compose -f docker-compose.builder.yml build
	docker-compose -f docker-compose.builder.yml run --rm crawler
stop:
	docker-compose -f docker-compose.builder.yml down
build:
	docker build -t derhenning/mopsy-crawler:latest .
schema:
	docker-compose -f docker-compose.builder.yml build
	docker-compose -f docker-compose.builder.yml run --rm schema