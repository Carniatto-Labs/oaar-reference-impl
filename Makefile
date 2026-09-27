COMPOSE := docker compose

.PHONY: up down ps logs preflight smoke langgraph-smoke abc-smoke abc-unknown-smoke hitl-smoke saga-smoke fanout-smoke guardrail-smoke mcp-smoke validate dash-smoke chat lock clean

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

hitl-smoke:
	$(COMPOSE) build app
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_hitl_resume && python -m oaar_impl.verify_hitl_resume"

saga-smoke:
	$(COMPOSE) build app
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_compensation_saga && python -m oaar_impl.verify_compensation_saga"

fanout-smoke:
	$(COMPOSE) build app
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_delegation_fanout && python -m oaar_impl.verify_delegation_fanout"

guardrail-smoke:
	$(COMPOSE) build app
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_guardrail_block && python -m oaar_impl.verify_guardrail_block"

mcp-smoke:
	$(COMPOSE) build app mcp-server
	$(COMPOSE) run --rm app sh -c "python -m oaar_impl.demo_mcp_retry_dedup && python -m oaar_impl.verify_mcp_retry_dedup"

validate:
	$(COMPOSE) build app product-api payment-api
	$(COMPOSE) run --rm app python -m oaar_impl.validate

chat:
	$(COMPOSE) build chat-panel product-api payment-api
	$(COMPOSE) up -d chat-panel
	@echo "Purchase assistant running at http://localhost:8090"

dash-smoke:
	$(COMPOSE) run --rm app python -m oaar_impl.verify_dashboards

lock:
	uv lock

clean:
	$(COMPOSE) down -v --remove-orphans
