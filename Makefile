.PHONY: build vet test race clean

build:
	go build -o kvdb ./src

vet:
	go vet ./src

test:
	go test ./src -v -count=1

race:
	go test ./src -v -race -count=1

clean:
	rm -f kvdb
	rm -f *.compact

