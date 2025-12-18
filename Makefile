PKG := ./...

.PHONY: all modtidy vet build test race cover coverhtml bench bench-all

all: test

modtidy:
	go mod tidy

vet:
	go vet $(PKG)

build:
	go build $(PKG)

test:
	go test $(PKG)

race:
	go test -race $(PKG)

cover:
	go test -cover $(PKG)

coverhtml:
	go test -coverprofile=coverage.out $(PKG)
	go tool cover -html=coverage.out

bench:
	go test -run '^$$' -bench . $(PKG)

bench-all:
	go test -run '^$$' -bench . -benchmem -benchtime=2s $(PKG)