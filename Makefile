.PHONY: build test race e2e fuzz bench bench-failure micro lint up down demo cluster stop

build:
	go build -o bin/ ./cmd/...

test:
	go test ./...

race:
	go test -race -count=1 ./...

e2e:
	SHC_E2E=1 go test -count=1 -v ./test/e2e/

fuzz:
	go test -run='^$$' -fuzz=FuzzReadFrame -fuzztime=30s ./internal/protocol/

lint:
	test -z "$$(gofmt -l .)" && go vet ./...

micro:
	go test -run='^$$' -bench=. -benchmem ./internal/store/ ./internal/ring/

bench: build
	./scripts/bench.sh
	./scripts/failover-timing.sh

cluster: build
	./scripts/local-cluster.sh start 3

stop:
	./scripts/local-cluster.sh stop

up:
	docker compose up -d --build

down:
	docker compose down

demo:
	./scripts/demo.sh

bench-failure: build
	./scripts/bench-failure.sh
