.PHONY: test
test:
	go test -v ./...

.PHONY: cover
cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

.PHONY: example
example: cover
	env GOOS=linux GOARCH=amd64 go build -o ./bin/example_linux ./example
	env GOOS=windows GOARCH=amd64 go build -o ./bin/example_windows.exe ./example