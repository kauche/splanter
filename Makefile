-include .env
SPANNER_EMULATOR_GRPC_PORT ?= 9010
SPANNER_EMULATOR_REST_PORT ?= 9020

BIN_DIR := ./.bin

GORELEASER_VERSION = 1.15.1

WRENCH := $(abspath $(BIN_DIR)/wrench)
GORELEASER := $(abspath $(BIN_DIR)/goreleaser-$(GORELEASER_VERSION))

EMULATOR_SPANNER_PROJECT = xxx
EMULATOR_SPANNER_INSTANCE = splanter-test
EMULATOR_SPANNER_DATABASE = splanter-test

wrench: $(WRENCH)
$(WRENCH): $(TOOLS_SUM)
	@cd tools && go build -o $(WRENCH) github.com/cloudspannerecosystem/wrench

goreleaser: $(GORELEASER)
$(GORELEASER):
	@curl -sSL "https://github.com/goreleaser/goreleaser/releases/download/v$(GORELEASER_VERSION)/goreleaser_$(shell uname -s)_$(shell uname -m).tar.gz" | tar -C $(BIN_DIR) -xzv goreleaser
	@mv ./$(BIN_DIR)/goreleaser $(GORELEASER)

.PHONY: test
test:
	@SPANNER_PROJECT=${SPANNER_PROJECT} \
		SPANNER_INSTANCE=${SPANNER_INSTANCE} \
		SPANNER_DATABASE=${SPANNER_DATABASE} \
		go test -race -shuffle=on ./...

.PHONY: test-docker
test-docker: $(WRENCH)
	docker compose stop
	docker compose up --detach

	docker run --rm --network splanter_default jwilder/dockerize:0.6.1 -wait tcp://spanner:9010 -timeout 10s

	SPANNER_EMULATOR_HOST=localhost:$(SPANNER_EMULATOR_GRPC_PORT) $(WRENCH) instance create \
		--project=$(EMULATOR_SPANNER_PROJECT) \
		--instance=$(EMULATOR_SPANNER_INSTANCE)
	SPANNER_EMULATOR_HOST=localhost:$(SPANNER_EMULATOR_GRPC_PORT) $(WRENCH) create \
		--project=$(EMULATOR_SPANNER_PROJECT) \
		--instance=$(EMULATOR_SPANNER_INSTANCE) \
		--database=$(EMULATOR_SPANNER_DATABASE) \
		--directory ./internal/spanner/testdata

	docker run \
		--rm \
		--volume "$(shell pwd):/src" \
		--workdir /src \
		--network splanter_default \
		--env SPANNER_EMULATOR_HOST=spanner:9010 \
		--env SPANNER_PROJECT=$(EMULATOR_SPANNER_PROJECT) \
		--env SPANNER_INSTANCE=$(EMULATOR_SPANNER_INSTANCE) \
		--env SPANNER_DATABASE=$(EMULATOR_SPANNER_DATABASE) \
		golang:1.20.0-bullseye make test

.PHONY: release
release: $(GORELEASER)
	$(GORELEASER) release
