COMPOSE := docker compose

.PHONY: up down ps logs preflight smoke langgraph-smoke abc-smoke abc-unknown-smoke lock clean

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down -v

ps:
	$(COMPOSE) ps

logs:
	$(COMPOSE) logs -f --tail=100

preflight:
	$(COMPOSE) run --rm app

smoke:
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_pipeline && python -m oaar_impl.verify_pipeline"

langgraph-smoke:
	$(COMPOSE) build app
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_langgraph && python -m oaar_impl.verify_langgraph"

abc-smoke:
	$(COMPOSE) build app
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_abc_checkpoint && python -m oaar_impl.verify_abc_checkpoint"

abc-unknown-smoke:
	$(COMPOSE) build app
	$(COMPOSE) run --rm app sh -c "OAAR_ABC_CONTEXT_PATH=/tmp/oaar-abc-unknown-context.json python -m oaar_impl.demo_variant_q_unknown && OAAR_ABC_CONTEXT_PATH=/tmp/oaar-abc-unknown-context.json python -m oaar_impl.verify_abc_checkpoint"

lock:
	uv lock

clean:
	$(COMPOSE) down -v --remove-orphans
