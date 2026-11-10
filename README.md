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
- v2.0.3
  - Decode every `ExportMetricsServiceRequest` in a Firehose record. A record can hold several length-prefixed messages, and only the first one was processed, so the rest of the metrics were dropped while Firehose still received 200
  - Log a warning when a record ends with a malformed message, and still ship the messages decoded before it
- v2.0.2
  - No functional changes (release process fix)
- v2.0.1
  - Report remote write failures to Firehose (non-200 response) instead of returning 200 and dropping the data, so Firehose retries the batch and backs it up to S3
  - Retry transient remote write failures (5xx, timeouts) with exponential backoff for up to 15 seconds per bulk, and stop exporting after 25 seconds so Firehose gets the error before the 30 second API Gateway timeout
  - Fix a nil error being collected on shipping failure
  - Add `aws_region` (from the Firehose source ARN) to each log
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