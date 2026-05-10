.PHONY: build test lint run clean tidy fmt vet broker sdk cli examples

BIN_DIR := bin
GO := go

# Module-aware operations across the workspace. The workspace root is not
# itself a module, so plain `./...` won't expand from the root — every
# fmt/vet/test/lint/tidy target iterates per-module. Keep this list in
# sync with go.work.
MODULES := proto sdk broker cli connect registry streams examples

build: broker cli

broker:
	$(GO) build -o $(BIN_DIR)/holocrond ./broker/cmd/holocrond

cli:
	$(GO) build -o $(BIN_DIR)/holocronctl ./cli/cmd/holocronctl

test:
	@for m in $(MODULES); do \
		echo "==> test $$m"; \
		(cd $$m && $(GO) test ./...) || exit 1; \
	done

fmt:
	@for m in $(MODULES); do \
		(cd $$m && $(GO) fmt ./...) || exit 1; \
	done

vet:
	@for m in $(MODULES); do \
		echo "==> vet $$m"; \
		(cd $$m && $(GO) vet ./...) || exit 1; \
	done

lint: fmt vet
	@command -v staticcheck >/dev/null || { echo "staticcheck not installed: go install honnef.co/go/tools/cmd/staticcheck@latest"; exit 1; }
	@for m in $(MODULES); do \
		echo "==> staticcheck $$m"; \
		(cd $$m && staticcheck ./...) || exit 1; \
	done

tidy:
	@for m in $(MODULES); do \
		echo "==> tidy $$m"; \
		(cd $$m && $(GO) mod tidy); \
	done

run: broker
	./$(BIN_DIR)/holocrond

clean:
	rm -rf $(BIN_DIR)
