.PHONY: up down test run run-rules gate-test report psql logs

RUN ?= run-$(shell date +%Y%m%d-%H%M%S)

up:            ## Levanta Postgres + Airflow (http://localhost:8080)
	docker volume create er_hf_cache >/dev/null
	docker compose up -d --build

down:
	docker compose down

test:          ## Tests unitarios
	docker compose exec -T airflow python -m pytest -q /opt/airflow/tests

run:           ## Run completo: baseline + Laya (english) -> Gold
	scripts/run_dag.sh $(RUN)

run-rules:     ## Solo reglas, sin Laya (~1 min)
	scripts/run_dag.sh $(RUN) '{"checkpoints": [], "primary": "baseline"}'

gate-test:     ## Inyecta un duplicado en staging: el DAG debe fallar y Gold no debe cambiar
	@before=$$(docker compose exec -T postgres psql -U airflow -d warehouse -Atc "$$(sed -n '/GOLD_HASH_SQL = """/,/"""/p' src/er/gold.py | sed '1d;$$d')"); \
	if scripts/run_dag.sh gate-$(RUN) '{"checkpoints": [], "primary": "baseline", "inject_duplicate": true}'; then \
	  echo "FALLO: el DAG no debió terminar OK"; exit 1; fi; \
	after=$$(docker compose exec -T postgres psql -U airflow -d warehouse -Atc "$$(sed -n '/GOLD_HASH_SQL = """/,/"""/p' src/er/gold.py | sed '1d;$$d')"); \
	if [ "$$before" = "$$after" ]; then echo "OK: el gate bloqueó la publicación y Gold no cambió ($$after)"; \
	else echo "FALLO: Gold cambió"; exit 1; fi

report:
	@cat reports/benchmark.md

psql:
	docker compose exec postgres psql -U airflow -d warehouse

logs:
	docker compose logs -f airflow
