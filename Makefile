.PHONY: up down reset test run logs psql

up:
	docker compose up -d --build

down:
	docker compose down

reset:
	docker compose down -v
	docker compose up -d --build

test:
	docker compose run --rm --build tests

run:
	docker compose run --rm --build pipeline

logs:
	docker compose logs -f --no-color

psql:
	docker compose exec db psql -U pipeline_user -d pipeline
