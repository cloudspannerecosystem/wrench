.PHONY: test
test:
	bazel test \
		--test_env SPANNER_PROJECT_ID=$$SPANNER_PROJECT_ID \
		--test_env SPANNER_INSTANCE_ID=$$SPANNER_INSTANCE_ID \
		--test_env SPANNER_DATABASE_ID=$$SPANNER_DATABASE_ID \
		--test_timeout 600 \
		--test_output streamed \
		--features race \
		//...

.PHONY: dep
dep:
	go mod tidy
	bazel run //:gazelle
	bazel run //:gazelle -- \
		update-repos \
		-from_file go.mod \
		-to_macro bazel/deps.bzl%wrench_deps

.PHONY: build
build:
	bazel build //:wrench

.PHONY: image
image:
	bazel build --platforms=@io_bazel_rules_go//go/toolchain:linux_amd64 //:image
