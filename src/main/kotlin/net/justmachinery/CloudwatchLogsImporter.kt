package net.justmachinery

import co.elastic.logstash.api.*
import com.google.gson.Gson
import org.logstash.Timestamp
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.cloudwatchlogs.CloudWatchLogsClient
import software.amazon.awssdk.services.cloudwatchlogs.model.FilterLogEventsRequest
import software.amazon.awssdk.services.cloudwatchlogs.model.FilteredLogEvent
import java.io.File
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.function.Consumer
import kotlin.math.max

@LogstashPlugin(name = "cloudwatch_logs_importer")
class CloudwatchLogsImporter(
    private val id : String,
    config : Configuration,
    private val context : Context
) : Input {
    private val accessKey : String? = config.get(ACCESS_KEY)
    private val secretKey : String? = config.get(SECRET_KEY)
    private val region : String? = config.get(REGION)
    private val targetEventsPerRequest = config.get(TARGET_EVENTS_PER_REQUEST)!!
    private val maxPollingInterval = config.get(MAX_POLLING_INTERVAL_MILLISECONDS)!!
    private val logGroups : List<String> = config.get(LOG_GROUPS).map { it as String }
    private val backwardsLogFetchDays : Long? = config.get(BACKWARDS_LOG_FETCH_DAYS)

    lateinit var client : CloudWatchLogsClient
    private val log = context.getLogger(this)

    override fun start(consumer: Consumer<MutableMap<String, Any>>) {
        val clientBuilder = CloudWatchLogsClient.builder()


        if(region != null){
            clientBuilder.region(Region.of(region))
        }

        if(accessKey != null || secretKey != null){
            require(!(accessKey == null || secretKey == null)) { "Both access_key and secret_key must be provided if either are" }
            clientBuilder.credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
        }


        client = clientBuilder.build()


        val processLog : (String, FilteredLogEvent)->Unit = { logGroup, it ->
            consumer.accept(mutableMapOf(
                "@timestamp" to Timestamp(it.timestamp()),
                "[cloudwatch_logs][ingestion_time]" to it.ingestionTime(),
                "[cloudwatch_logs][log_group]" to logGroup,
                "[cloudwatch_logs][log_stream]" to it.logStreamName(),
                "[cloudwatch_logs][event_id]" to it.eventId(),
                "message" to it.message()
            ))
        }

        var interval = 0L
        var backwardsWindow = 60 * 60 * 1000L
        while(stopped.count > 0){
            try {
                val seenForward = requestForwardLogs(processLog)
                val seenBackward = requestBackwardLogs(processLog, backwardsWindow)

                backwardsWindow = if (seenBackward < targetEventsPerRequest) {
                    (backwardsWindow * 1.2).toLong().coerceAtMost(365L * 24 * 60 * 60 * 1000)
                } else {
                    (backwardsWindow / 1.2).toLong().coerceAtLeast(1000)
                }

                interval = if (seenForward + seenBackward < targetEventsPerRequest) {
                    ((interval + 1000) * 1.2).toLong().coerceAtMost(maxPollingInterval)
                } else {
                    0
                }

                log.debug("Ingested $seenForward forward entries, $seenBackward backward entries (new backwards window $backwardsWindow ms). Sleeping for $interval ms")
            } catch(e : Exception){
                log.error("Could not fetch", e)
                interval = ((interval + 1000) * 1.2).toLong().coerceAtMost(maxPollingInterval)
            }
            stopped.await(interval, TimeUnit.MILLISECONDS)
        }
        stoppingDone.countDown()
    }

    private fun requestForwardLogs(cb : (String, FilteredLogEvent)->Unit) : Int {
        val request = FilterLogEventsRequest.builder()
        request.startTime(data.latestTimestampSeen)
        request.endTime(Instant.now().toEpochMilli())
        var seen = 0
        logGroups.forEach { logGroup ->
            request.logGroupName(logGroup)
            var nextToken : String? = null
            while(true) {
                if(nextToken != null){
                    request.nextToken(nextToken)
                } else {
                    request.nextToken(null)
                }
                val response = client.filterLogEvents(request.build())
                response.events().forEach { event ->
                    cb(logGroup, event)
                    data.latestTimestampSeen = max(event.timestamp(), data.latestTimestampSeen)
                    seen += 1
                }
                nextToken = response.nextToken()
                if(nextToken == null) break
            }
        }
        return seen
    }

    private fun requestBackwardLogs(cb : (String, FilteredLogEvent)->Unit, backwardsWindow : Long) : Int {
        if(data.earliestTimestampSeen == 0L){ return 0 }
        if(backwardsLogFetchDays != null && data.earliestTimestampSeen < Instant.now().minus(backwardsLogFetchDays, ChronoUnit.DAYS).toEpochMilli()){
            data.earliestTimestampSeen = 0
            return 0
        }

        val earlier = max(data.earliestTimestampSeen - backwardsWindow, 0)

        val request = FilterLogEventsRequest.builder()
        request.startTime(earlier)
        request.endTime(data.earliestTimestampSeen)

        var seen = 0
        logGroups.forEach { logGroup ->
            request.logGroupName(logGroup)
            var nextToken : String? = null
            while(true) {
                if(nextToken != null){
                    request.nextToken(nextToken)
                } else {
                    request.nextToken(null)
                }
                val response = client.filterLogEvents(request.build())
                response.events().forEach { event ->
                    cb(logGroup, event)
                    seen += 1
                }
                nextToken = response.nextToken()
                if(nextToken == null) break
            }
        }

        data.earliestTimestampSeen = earlier

        return seen
    }

    private val stopped = CountDownLatch(1)
    private val stoppingDone = CountDownLatch(1)

    override fun stop() {
        log.info("Stop requested")
        savePluginData()
        stopped.countDown()
    }

    override fun awaitStop() {
        stoppingDone.await()
        log.info("Stopped")
    }



    private val pluginDataPath = File(config.get(PLUGIN_DATA_DIRECTORY)!!).resolve("cloudwatch_logs_importer_data.json")
    private val data = loadPluginData()

    private fun loadPluginData() : PluginData {
        return if(pluginDataPath.exists()){
            Gson().fromJson(pluginDataPath.readText(), PluginData::class.java).also {
                log.info("Loaded plugin data: $data")
            }
        } else {
            val now = Instant.now().toEpochMilli()
            PluginData(now, now).also {
                log.info("Created new plugin data")
            }
        }
    }
    private fun savePluginData() {
        try {
            pluginDataPath.writeText(Gson().toJson(data))
        } catch(e : Exception){
            log.error("Cannot save plugin data. Check that plugin_data_directory is writable and exists.", e)
        }
    }


    override fun getId() = id
    override fun configSchema() = setOf(
        REGION, ACCESS_KEY, SECRET_KEY, LOG_GROUPS,
        PLUGIN_DATA_DIRECTORY,
        TARGET_EVENTS_PER_REQUEST, MAX_POLLING_INTERVAL_MILLISECONDS,
        BACKWARDS_LOG_FETCH_DAYS
    )

    companion object {
        val REGION = PluginConfigSpec.stringSetting("region")!!
        val ACCESS_KEY = PluginConfigSpec.stringSetting("access_key")!!
        val SECRET_KEY = PluginConfigSpec.stringSetting("secret_key")!!
        val LOG_GROUPS = PluginConfigSpec.arraySetting("log_groups", null, false, true)!!
        val PLUGIN_DATA_DIRECTORY = PluginConfigSpec.stringSetting("plugin_data_directory", null, false, true)!!
        val TARGET_EVENTS_PER_REQUEST = PluginConfigSpec.numSetting("target_events_per_request", 5000)!!
        val MAX_POLLING_INTERVAL_MILLISECONDS = PluginConfigSpec.numSetting("max_polling_interval_milliseconds", 120_000)!!
        val BACKWARDS_LOG_FETCH_DAYS = PluginConfigSpec.numSetting("backwards_log_fetch_days")!!
    }
}

private data class PluginData(
    var latestTimestampSeen : Long,
    var earliestTimestampSeen : Long
)