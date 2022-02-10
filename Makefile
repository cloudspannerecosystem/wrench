bin/bazelisk: tools.go
	go build -o bin/bazelisk github.com/bazelbuild/bazelisk

.PHONY: test
test: bin/bazelisk
	bin/bazelisk test \
		--test_env SPANNER_PROJECT_ID=$$SPANNER_PROJECT_ID \
		--test_env SPANNER_INSTANCE_ID=$$SPANNER_INSTANCE_ID \
		--test_env SPANNER_DATABASE_ID=$$SPANNER_DATABASE_ID \
		--test_timeout 600 \
		--test_output streamed \
		--features race \
		//...

.PHONY: dep
dep: bin/bazelisk
	go mod tidy
	bin/bazelisk run //:gazelle -- -exclude vendor
	bin/bazelisk run //:gazelle -- \
		update-repos \
		-build_file_proto_mode=disable_global \
		-from_file go.mod \
		-to_macro bazel/deps.bzl%wrench_deps \
		-prune

.PHONY: build
build: bin/bazelisk
	bin/bazelisk build //:wrench

.PHONY: image
image: bin/bazelisk
	bin/bazelisk build --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 //:image

.PHONY: registry
registry: bin/bazelisk
	bin/bazelisk run --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 //:registry
