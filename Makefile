BIN_DIR := ./.bin
WRENCH := $(abspath $(BIN_DIR)/wrench)

wrench: $(WRENCH)
$(WRENCH): $(TOOLS_SUM)
	@cd tools && go build -o $(WRENCH) github.com/cloudspannerecosystem/wrench
