dev-up:
	docker-compose -f docker-compose.dev.yml up -d

prod-up:
	docker-compose up -d

load-data:
	docker-compose exec -it etl sh -c "python etl.py"

full-up:
	make dev-up
	make load-data

stop:
	docker-compose stop

remove:
	docker-compose down

remove-all:
	docker-compose down -v
