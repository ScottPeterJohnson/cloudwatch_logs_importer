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
import kotlin.concurrent.thread
import kotlin.math.max
import kotlin.math.min

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

        thread { //Periodically save plugin data, in case of a crash or something
            while(stopped.count > 0){
                if(!stopped.await(30, TimeUnit.MINUTES)){
                    savePluginData()
                }
            }
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
        try {
            while(stopped.count > 0){
                try {
                    synchronized(this){
                        val seenForward = requestForwardLogs(processLog)
                        val seenBackward = requestBackwardLogs(processLog)

                        interval = if (seenForward + seenBackward < targetEventsPerRequest) {
                            ((interval + 1000) * 1.2).toLong().coerceAtMost(maxPollingInterval)
                        } else {
                            0
                        }
                        log.info("Ingested $seenForward forward entries, $seenBackward backward entries. Sleeping for $interval ms")
                    }
                } catch(e : Exception){
                    log.error("Could not fetch", e)
                    interval = ((interval + 1000) * 1.2).toLong().coerceAtMost(maxPollingInterval)
                }
                stopped.await(interval, TimeUnit.MILLISECONDS)
            }
        } finally {
            stoppingDone.countDown()
            log.info("Stopped ingesting")
        }
    }

    private fun requestForwardLogs(cb : (String, FilteredLogEvent)->Unit) : Int {
        var seen = 0
        logGroups.forEach { logGroup ->
            val request = FilterLogEventsRequest.builder()
            request.logGroupName(logGroup)

            val now = Instant.now().toEpochMilli()
            val start = synchronized(data){
                val current = data.logGroupLastForward.getOrPut(logGroup){ 0L }
                val fiveMinutesAgo = now - 300_000
                //If we're more than five minutes behind, skip and create a gap
                if(current < fiveMinutesAgo){
                    logGroupTimestamps(logGroup).add(
                        TimestampRange(
                            lower = current,
                            upper = fiveMinutesAgo
                        )
                    )
                    data.logGroupLastForward[logGroup] = fiveMinutesAgo
                    fiveMinutesAgo
                } else {
                    current
                }
            }

            request.startTime(start)
            request.endTime(now)

            var nextToken : String? = null
            while(stopped.count > 0) {
                if(nextToken != null){
                    request.nextToken(nextToken)
                } else {
                    request.nextToken(null)
                }
                val response = client.filterLogEvents(request.build())
                response.events().forEach { event ->
                    cb(logGroup, event)
                    data.logGroupLastForward[logGroup] = max(event.timestamp(), data.logGroupLastForward[logGroup] ?: 0L)
                    seen += 1
                }
                nextToken = response.nextToken()
                if(nextToken == null) break
            }
            if(stopped.count > 0){
                data.logGroupLastForward[logGroup] = max(now, data.logGroupLastForward[logGroup] ?: 0L)
            }
        }
        return seen
    }

    private val backwardsLogWindows = mutableMapOf<String, Long>()
    private fun requestBackwardLogs(cb : (String, FilteredLogEvent)->Unit) : Int {
        var seen = 0
        logGroups.forEach { logGroup ->
            fun processLogGroup() : Boolean {
                val backwardsWindow = backwardsLogWindows.getOrPut(logGroup){ 60_000L}
                val lastRange = synchronized(data){
                    val ranges = logGroupTimestamps(logGroup)
                    val current = ranges.lastOrNull() ?: return false
                    current
                }
                val earliestAllowed = backwardsLogFetchDays?.let { Instant.now().minus(it, ChronoUnit.DAYS).toEpochMilli() } ?: 0L
                if(lastRange.upper < earliestAllowed){ return false }
                val requestStart = (lastRange.upper - backwardsWindow).coerceAtLeast(lastRange.lower).coerceAtLeast(earliestAllowed)
                val request = FilterLogEventsRequest.builder().apply {
                    startTime(requestStart)
                    endTime(lastRange.upper)
                    logGroupName(logGroup)
                }

                var logGroupSeen = 0
                var nextToken : String? = null
                while(stopped.count > 0) {
                    if(nextToken != null){
                        request.nextToken(nextToken)
                    } else {
                        request.nextToken(null)
                    }
                    val response = client.filterLogEvents(request.build())
                    response.events().forEach { event ->
                        cb(logGroup, event)
                        logGroupSeen += 1
                    }
                    nextToken = response.nextToken()
                    if(nextToken == null) break
                }
                if(stopped.count > 0){ //Only update if this range request finished
                    lastRange.upper = requestStart
                    //Merge overlapping ranges from back to front:
                    synchronized(data){
                        val ranges = logGroupTimestamps(logGroup)
                        data.logGroupTimestamps[logGroup] = ranges.foldRight<TimestampRange, List<TimestampRange>>(listOf()){ earlier, acc ->
                            if (earlier.upper <= earlier.lower) {
                                acc
                            } else if (acc.isEmpty()) {
                                listOf(earlier)
                            } else {
                                val later = acc.last()
                                if (later.lower <= earlier.upper) {
                                    acc.dropLast(1) + TimestampRange(
                                        lower = min(later.lower, earlier.lower),
                                        upper = max(later.upper, earlier.upper)
                                    )
                                } else if (earlier.upper < earliestAllowed) {
                                    acc
                                } else {
                                    acc + earlier
                                }
                            }
                        }.asReversed().toMutableList()
                    }

                    backwardsLogWindows[logGroup] = if (logGroupSeen < targetEventsPerRequest) {
                        (backwardsWindow * 1.2).toLong().coerceAtMost(365L * 24 * 60 * 60 * 1000)
                    } else {
                        (backwardsWindow / 1.2).toLong().coerceAtLeast(1000)
                    }

                }

                seen += logGroupSeen

                return true
            }
            @Suppress("ControlFlowWithEmptyBody")
            while(stopped.count > 0 && seen < targetEventsPerRequest && processLogGroup()){}
        }

        return seen
    }

    private fun logGroupTimestamps(logGroup : String) = data.logGroupTimestamps.getOrPut(logGroup){ mutableListOf() }

    private val stopped = CountDownLatch(1)
    private val stoppingDone = CountDownLatch(1)

    override fun stop() {
        log.info("Stop requested")
        stopped.countDown()
    }

    override fun awaitStop() {
        stoppingDone.await()
        savePluginData()
        log.info("Stopped")
    }



    private val pluginDataPath = File(config.get(PLUGIN_DATA_DIRECTORY)!!).resolve("cloudwatch_logs_importer_data.json")
    private val data = loadPluginData()

    private fun loadPluginData() : PluginData {
        return if(pluginDataPath.exists()){
            try {
                Gson().fromJson(pluginDataPath.readText(), PluginData::class.java).also {
                    log.info("Loaded plugin data: $it")
                }!!
            } catch(t : Throwable){
                log.error("Plugin data was corrupt or outdated, starting from scratch")
                PluginData()
            }
        } else {
            PluginData().also {
                log.info("Created new plugin data")
            }
        }
    }
    private fun savePluginData() {
        try {
            synchronized(this){
                pluginDataPath.writeText(Gson().toJson(data))
            }
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
    val logGroupTimestamps : MutableMap<String, MutableList<TimestampRange>> = mutableMapOf(),
    val logGroupLastForward : MutableMap<String, Long> = mutableMapOf()
)

private data class TimestampRange(
    var lower : Long,
    var upper : Long
)