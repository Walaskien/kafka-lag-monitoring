PROM_IMAGE ?= prom/prometheus:v2.55.1
AM_IMAGE   ?= prom/alertmanager:v0.27.0
PROMTOOL    = docker run --rm -v $(CURDIR)/prometheus:/p:ro --entrypoint promtool $(PROM_IMAGE)
COMPOSE    ?= docker compose

.PHONY: up down test lint check scenario describe consumer-pause consumer-resume consumer-slow consumer-fast alerts

up:
	$(COMPOSE) up -d --build
	@echo "Grafana http://localhost:13000  Prometheus http://localhost:19090  Alertmanager http://localhost:19093"

down:
	$(COMPOSE) down -v

test:
	python3 -m pytest -q

lint:
	ruff check .

check:
	$(PROMTOOL) check config /p/prometheus.yml
	$(PROMTOOL) check rules /p/rules/recording.yml /p/rules/alerts.yml
	$(PROMTOOL) test rules /p/tests.yml
	docker run --rm -v $(CURDIR)/alertmanager:/a:ro --entrypoint amtool $(AM_IMAGE) check-config /a/alertmanager.yml
	docker run --rm -v $(CURDIR):/mnt -w /mnt koalaman/shellcheck-alpine:stable shellcheck scripts/*.sh
	docker run --rm -v $(CURDIR):/mnt -w /mnt rhysd/actionlint:1.7.7
	$(COMPOSE) config -q

# the full incident walk-through against a running stack, about 8 minutes
scenario:
	scripts/scenario.sh

describe:
	$(COMPOSE) exec -T kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group orders-processor

consumer-pause:
	curl -fsS -X POST localhost:19102/pause
consumer-resume:
	curl -fsS -X POST localhost:19102/resume
consumer-slow:
	curl -fsS -X POST localhost:19102/process-ms/100
consumer-fast:
	curl -fsS -X POST localhost:19102/process-ms/0

alerts:
	@python3 tools/promq.py snapshot
	@python3 tools/promq.py am-alerts
