BIN_DIR := ./.bin
WRENCH := $(abspath $(BIN_DIR)/wrench)

.PHONY: test
test:
	@SPANNER_PROJECT=${SPANNER_PROJECT} \
		SPANNER_INSTANCE=${SPANNER_INSTANCE} \
		SPANNER_DATABASE=${SPANNER_DATABASE} \
		go test -race -shuffle=on ./...

wrench: $(WRENCH)
$(WRENCH): $(TOOLS_SUM)
	@cd tools && go build -o $(WRENCH) github.com/cloudspannerecosystem/wrench
