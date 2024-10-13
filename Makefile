build:
	go build main.go

function:
	GOARCH=amd64 GOOS=linux go build -o bootstrap main.go
	zip -r function.zip bootstrap
