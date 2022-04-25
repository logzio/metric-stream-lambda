build:
	go build main.go

function:
	GOARCH=amd64 GOOS=linux go build main.go
	zip -r function.zip main
