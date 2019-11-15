[![Gem][ico-version]][link-rubygems]

# Logstash Cloudwatch Logs Importer Input

Plugin to import logs from Cloudwatch Logs. Requires Logstash 7+.

If you encounter any problems, please file an issue! Contributions are also welcome.

See the [Logstash Java input plugin](https://www.elastic.co/guide/en/logstash/current/java-input-plugin.html) guide for instructions on compiling the project.

## How it works
Starting from its time of activation, this plugin will attempt to continuously import 
both new and old logs.

Note that you will want to deduplicate your logs at some point in your Logstash pipeline, as some log events will be imported twice.


## Parameters

### Required

| Parameter | Input Type | Description |
|-----------|------------|---------|
| log_groups | array of strings | List of log group names to import logs from. You may need to delete data files in the plugin data directory to restart the import process if you add a new log group. |
| plugin_data_directory | string | Writable directory to store plugin data in |
 

### Optional

| Parameter | Input Type | Default | Description |
|-----------|------------|---------|---------| 
| target_events_per_request | number | 5000 | Polling rate slows down if cloudwatch logs returns fewer than this many logs per request |
| max_polling_interval_milliseconds | number | 120000 (two minutes) | Longest interval allowed before checking for new events. |
| backwards_log_fetch_days | number | none | Maximum age of old logs to fetch. |

### AWS Credentials
If not supplied, these will come from the [default provider chain.](
https://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/credentials.html#credentials-default)

| Parameter | Input Type |
|-----------|------------|
| region | string |
| access_key | string |
| secret_key | string |

[link-rubygems]: https://rubygems.org/gems/logstash-input-cloudwatch_logs