.PHONY: test vet race build bench check

test:
	go test ./...

vet:
	go vet ./...

race:
	go test -race ./...

build:
	go build ./cmd/sqlite-stored

bench:
	go test -run '^$$' -bench . -benchmem ./benchmarks

check: test vet race build
