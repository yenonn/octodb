.PHONY: build run test test-integration test-block1 test-block2 clean

BINARY := octodb
GO := go

testclient := bin/testclient

build:
	@mkdir -p bin
	$(GO) build -o bin/$(BINARY) ./cmd/octodb
	$(GO) build -o $(testclient) ./cmd/testclient

run: build
	./bin/$(BINARY)

test:
	$(GO) test ./...

# Run integration tests that compile the server binary and exercise it end-to-end.
test-integration:
	$(GO) test ./tests/integration/ -v -timeout 60s

# Run only Block 1 integration test (WAL crash recovery).
test-block1:
	$(GO) test ./tests/integration/ -v -run TestBlock1_WALCrashRecovery -timeout 30s

# Run only Block 2 integration test (memtable flush + query).
test-block2:
	$(GO) test ./tests/integration/ -v -run TestBlock2_FlushAndQuery -timeout 60s

clean:
	rm -rf bin/ *.wal octodb-data/ *.log *.pb
