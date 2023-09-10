# metric-stream-lambda
Lambda function that receives OTLP (0.7.0) data from AWS metric stream and exports the data to logz.io using prometheus remote write

### How to create function.zip
```
make function
```

### How it Works
- The function begins by importing a number of packages, including those for handling AWS events, `encoding/decoding` `JSON`, working with OpenTelemetry data, and logging. 
- The `firehoseResponse` struct is used to hold information about the response to the Lambda function's API Gateway trigger, including the request ID, a timestamp, and an error message. The `generateValidFirehoseResponse` function takes in a status code, request ID, error message, and error, and returns an `events.APIGatewayProxyResponse` with the given information and some default headers and other values.
- The `initLogger` function initializes a logger using Uber's zap package, taking in a context and an `events.APIGatewayProxyRequest` as arguments. It extracts the AWS request ID and account ID from the context and request, respectively, and uses them to configure the logger.
- The `HandleRequest` function is the main entry point for the Lambda function. It receives an `events.APIGatewayProxyRequest` and a context, and returns an `events.APIGatewayProxyResponse`. It first initializes the logger using the initLogger function. It then decodes the base64-encoded request body and unmarshals it into a `pb.ExportMetricsServiceRequest` protobuf message.
- Next, the function creates a `pdata.Metrics` value from the protobuf message and a `componenttest.NewNopExporter` component to hold the metrics data. It then creates a `prometheusremotewriteexporter.Exporter` using the `getListenerUrl` function to get the logz.io listener url and the `LogzioToken` variable extracted from the `events.APIGatewayProxyRequest` headers.
- The function then iterates through the metrics in the `pdata.Metrics` value, extracting their names and data points. It converts the data points into Prometheus Remote Write format and sends them to the exporter. If any errors are encountered during this process, they are collected in an `ErrorCollector`.
- Finally, the function checks the length of the `ErrorCollector` and returns an appropriate response based on whether any errors were encountered. If there were no errors, a response with a status code of 200 and an empty error message is returned. If there were errors, a response with a status code of 400 and the concatenated error messages is returned.

### Limitations
This function has the following limitations:

- It can only process metrics data in OTLP 0.7 format.
- It can only forward the data to a Prometheus Remote Write endpoint.

### Changelog

- v1.0.2
  - Stop trying to send bulks if encountered 401 status code
  - Add logzio identifier to each log (5 last chars of the shipping token)
  - Add zip workflow and artifact
- v1.0.1
  - Improved logging (Add `zap` logger)
  - Add metadata (AWS account, firehose request id, lambda invocation id) to each log for context
  - Flush buffered logs if exists, before the function run ends
- v1.0.0
  - Initial release: Lambda function that receives OTLP (0.7.0) data from AWS metric stream and exports the data to logz.io using Prometheus remote write