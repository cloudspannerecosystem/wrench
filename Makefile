BINDIR ?= bin

export SPANNER_EMULATOR_HOST := localhost:9010
SPANNER_EMULATOR_HOST_REST := localhost:9020

export SPANNER_PROJECT_ID ?= wrench-test-project
export SPANNER_INSTANCE_ID ?= wrench-test-instance

REGISTRY := mercari/wrench

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

setup-emulator:
	curl -s "${SPANNER_EMULATOR_HOST_REST}/v1/projects/${SPANNER_PROJECT_ID}/instances" --data '{"instanceId": "'${SPANNER_INSTANCE_ID}'"}'

docker-build:
	docker build . -t $(REGISTRY):$(VERSION) --build-arg VERSION=$(VERSION)
