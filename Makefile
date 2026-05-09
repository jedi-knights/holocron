.PHONY: build test lint run clean tidy fmt vet broker sdk cli examples

BIN_DIR := bin
GO := go

# Module-aware operations across the workspace. The workspace root is not
# itself a module, so plain `./...` won't expand — we list each module path.
MODULES := proto broker sdk cli examples
PKGS := ./broker/... ./cli/... ./examples/... ./proto/... ./sdk/...

build: broker cli

broker:
	$(GO) build -o $(BIN_DIR)/holocrond ./broker/cmd/holocrond

cli:
	$(GO) build -o $(BIN_DIR)/holocronctl ./cli/cmd/holocronctl

test:
	$(GO) test $(PKGS)

fmt:
	$(GO) fmt $(PKGS)

vet:
	$(GO) vet $(PKGS)

lint: fmt vet
	@command -v staticcheck >/dev/null || { echo "staticcheck not installed: go install honnef.co/go/tools/cmd/staticcheck@latest"; exit 1; }
	staticcheck $(PKGS)

tidy:
	@for m in $(MODULES); do \
		echo "==> $$m"; \
		(cd $$m && $(GO) mod tidy); \
	done

run: broker
	./$(BIN_DIR)/holocrond

clean:
	rm -rf $(BIN_DIR)
