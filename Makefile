.PHONY: up down reset logs psql password test

up:        ## build and start everything
	docker compose up -d --build

down:      ## stop everything (keeps data)
	docker compose down

reset:     ## stop everything and delete all data
	docker compose down -v

logs:      ## follow the streaming services
	docker compose logs -f producer consumer

psql:      ## open a SQL shell
	docker compose exec postgres psql -U crypto

password:  ## print the Airflow admin password
	docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt

test:      ## lint and unit tests
	ruff check . && pytest -q
