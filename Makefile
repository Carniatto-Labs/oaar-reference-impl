COMPOSE := docker compose

.PHONY: up down ps logs preflight lock clean

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

lock:
	uv lock

clean:
	$(COMPOSE) down -v --remove-orphans
