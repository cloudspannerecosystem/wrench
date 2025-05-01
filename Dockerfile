FROM gcr.io/distroless/static-debian12

# The binary is built by goreleaser
COPY wrench /

ENTRYPOINT ["/wrench"]
