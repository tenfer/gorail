all: build

build:
	go build -o ./bin/rail ./cmd-rail
clean:
	@rm -rf bin
test:
	go test ./go/... -race
