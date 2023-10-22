BIN="./bin"
SRC=$(shell find . -name "*.go")

ifeq (, $(shell which golangci-lint))
$(warning "could not find golangci-lint in $(PATH), run: curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.55.0")
endif

.PHONY: fmt lint test install_deps clean

default: all

all: fmt test

fmt:
	@gofmt -d $(SRC)

lint:
	@golangci-lint run -v

build: tidy
	@go build -o bin/ddbctl ./cmd/main.go

run: build
	@./bin/ddbctl

test: tidy
	@go test -v ./...

tidy:
	@go mod tidy

docker_build:
	@docker build -t go-dynamodb-partition-delete .

docker_hub_build:	
	@docker build -t jittakal/go-dynamodb-partition-delete .

clean:
	@rm -rf ${BIN}