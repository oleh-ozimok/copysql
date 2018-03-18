BINARY = ../../copysql
GOARCH = amd64

VERSION?=?
COMMIT=$(shell git rev-parse HEAD)
BRANCH=$(shell git rev-parse --abbrev-ref HEAD)

BUILD_DIR=${GOPATH}/src/github.com/oleh-ozimok/copysql/cmd/copysql
CURRENT_DIR=$(shell pwd)
BUILD_DIR_LINK=$(shell readlink ${BUILD_DIR})

# Setup the -ldflags option for go build here, interpolate the variable values
LDFLAGS = -ldflags "-X main.VERSION=${VERSION} -X main.COMMIT=${COMMIT} -X main.BRANCH=${BRANCH}"

all: clean test vet linux darwin

linux:
	cd ${BUILD_DIR}; \
	GOOS=linux GOARCH=${GOARCH} go build ${LDFLAGS} -o ${BINARY}-linux-${GOARCH} . ; \
	cd - >/dev/null

darwin:
	cd ${BUILD_DIR}; \
	GOOS=darwin GOARCH=${GOARCH} go build ${LDFLAGS} -o ${BINARY}-darwin-${GOARCH} . ; \
	cd - >/dev/null

test:
	cd ${BUILD_DIR}/../..; \
	go test -v ./...; \
	cd - >/dev/null
vet:
	-cd ${BUILD_DIR}/../..; \
	go vet $$(go list ./... | grep -v /vendor/); \
	cd - >/dev/null

fmt:
	cd ${BUILD_DIR}/../..; \
	go fmt $$(go list ./... | grep -v /vendor/) ; \
	cd - >/dev/null

clean:
	-rm -f ${BINARY}-*


.PHONY: linux darwin test vet fmt clean