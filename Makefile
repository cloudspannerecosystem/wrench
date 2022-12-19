BINDIR ?= bin

.PHONY: test
test:
	go test -race -v ./...

.PHONY: dep
dep:
	go mod tidy

.PHONY: build
build:
	@mkdir -p $(BINDIR)
	go build -o $(BINDIR)/wrench .
