# metric-stream-lambda
Lambda function that receives OTLP (0.7.0) data from AWS metric stream and exports the data to logz.io using prometheus remote write

## How to create function.zip
* `GOARCH=amd64 GOOS=linux go build main.go`
* `zip -r function.zip main`
