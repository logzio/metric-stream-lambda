# metric-stream-lambda
Lambda function that receives OTLP (0.7.0) data from AWS metric stream and exports the data to logz.io using prometheus remote write

## How to create function.zip
* `GOARCH=amd64 GOOS=linux go build main.go`
* `zip -r function.zip main`
## Deploy to AWS with cli
After creating `function.zip`
### Dev 
```shell
aws lambda update-function-code \
--region us-east-1 \
--function-name cloudwatch-stream-otlp-dev \
--zip-file fileb://function.zip
```
### Prod
```shell
aws lambda update-function-code \
--region us-east-1 \
--function-name cloudwatch-stream-otlp \
--zip-file fileb://function.zip
```
