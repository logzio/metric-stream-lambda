# metric-stream-lambda
Lambda function that receives OTLP (1.0) data from AWS metric stream and exports the data to logz.io using prometheus remote write

### How to create function.zip
```
make function
```

### Limitations
This function has the following limitations:

- It can only process metrics data in OTLP 1.0 format.
- It can only forward the data to a Prometheus Remote Write endpoint.

### Changelog
- v2.0.0
  - Breaking changes:
    - Move from otlp 0.7 -> otlp 1.0
  - Add support for custom p8s_logzio_name label
  - Performance improvements
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