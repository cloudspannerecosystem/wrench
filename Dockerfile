FROM golang:1.24 as build

ARG VERSION

WORKDIR /go/src/app
COPY . .

RUN go mod download
RUN CGO_ENABLED=0 go build \
    -ldflags "-s -w -X github.com/cloudspannerecosystem/wrench/cmd.version=${VERSION}" \
    -o /go/bin/app/wrench

FROM gcr.io/distroless/static-debian12
COPY --from=build /go/bin/app/wrench /
ENTRYPOINT ["/wrench"]
