package me.vitaly.etl.jobs

import com.google.common.annotations.VisibleForTesting
import me.vitaly.etl.model.RawLog
import me.vitaly.etl.model.SessionLog
import org.apache.spark.sql.*
import org.apache.spark.sql.functions.*
import org.apache.spark.sql.types.DataTypes.TimestampType
import org.jetbrains.kotlinx.spark.api.*
import org.jetbrains.kotlinx.spark.api.SparkSession
import java.util.concurrent.TimeUnit

// Представим, что есть набор данных со следующей структурой:
//
//    device_id;
//    timestamp;
//    event;
//    product.
//
// Новая часть данных приходит с началом очередных суток и может содержать в себе данные не более чем за 5 предыдущих дней.
// Необходимо создать решение для обогащения данных идентификатором сессии, который строится по принципу
// device_id#product#timestamp. Timestamp соответствует началу сессии.
// События в сессии могут быть как пользовательскими, так и служебными.
// Идентификатор присваивается всем событиям.
// Сессия начинается с события, которое является пользовательским.
// Пусть пользовательскими будут события, для которых event in (‘a’, ‘b’, ‘c’).
//
// Сессия прерывается, если в течение 5 минут в ней не случалось пользовательских событий.
// Сессии необходимо строить с учетом уже имеющихся данных (например, сегодня мы могли получить “хвост” сессии,
// начало которой встретили 3 дня назад).
object SessionLogsEnrichmentJob {
    fun run(
        spark: SparkSession,
        rawLogFiles: Set<String>,
        sessionLogFiles: Set<String>,
        resultPath: String,
        sessionMaxMinutesBetweenEvents: Int,
        userEvents: Set<String>,
    ) {
        val rawLogDataset = readRawLogDataset(spark, rawLogFiles)
        val sessionDataset = if (sessionLogFiles.isEmpty())
            spark.emptyDataset(encoder<SessionLog>())
        else readSessionLogDataset(spark, sessionLogFiles)

        enrichLogs(rawLogDataset, sessionDataset, userEvents, sessionMaxMinutesBetweenEvents)
            .writeWithPartitions(resultPath)
    }

    @VisibleForTesting
    /**
     * Main transformations without read/write operations
     */
    internal fun enrichLogs(
        rawLogDataset: Dataset<RawLog>,
        sessionDataset: Dataset<SessionLog>,
        userEvents: Set<String>,
        sessionMaxMinutesBetweenEvents: Int
    ): Dataset<SessionLog> {
        return rawLogDataset
            .map { it.toSessionLog() }
            .union(sessionDataset)
            // looks like in general we can group by these keys without OOM problems due to high-cardinality of these values
            .groupByKey { c((it.product), it.device_id) }
            .flatMapGroups { _, iterator ->
                fillSession(
                    iterator,
                    userEvents,
                    TimeUnit.MINUTES.toMillis(sessionMaxMinutesBetweenEvents.toLong())
                )
            }

    }

    /**
     * Takes combined raw and session logs, grouped by product and device_id.
     * Returns Session Logs with filled session_id only for new logs.
     */
    private fun fillSession(
        iterator: Iterator<SessionLog>,
        userEvents: Set<String>,
        sessionMaxMillisBetweenEvents: Long
    ): Iterator<SessionLog> {
        var sessionId: String? = null
        var lastSessionEventTimestamp: Long? = null
        return iterator.asSequence()
            // sorting inside the group is required to calculate the session
            .sortedBy { it.timestamp }
            .mapNotNull { row ->
                when {
                    isAlreadyProcessedLog(row) -> {
                        sessionId = if (row.session_id == NA_VALUE) null else row.session_id
                        lastSessionEventTimestamp = if (row.session_id == NA_VALUE) null else row.timestamp
                        // session logs required only for calculations, they should not be stored
                        return@mapNotNull null
                    }
                    isNeededToStartSession(lastSessionEventTimestamp, row, sessionMaxMillisBetweenEvents) ->
                        if (row.event in userEvents) {
                            sessionId = buildSessionId(row.device_id, row.product, row.timestamp)
                            lastSessionEventTimestamp = row.timestamp
                        } else {
                            // set no active sessions
                            sessionId = null
                            lastSessionEventTimestamp = null
                        }
                    // continue old session
                    else -> lastSessionEventTimestamp = row.timestamp
                }
                SessionLog(
                    device_id = row.device_id,
                    timestamp = row.timestamp,
                    event = row.event,
                    product = row.product,
                    session_id = sessionId ?: NA_VALUE
                )
            }.iterator()
    }

    internal fun buildSessionId(deviceId: String, productId: String, timestamp: Long) = "$deviceId#$productId#$timestamp"

    private fun isAlreadyProcessedLog(row: SessionLog) = row.session_id != null

    private fun isNeededToStartSession(
        lastSessionEventTimestamp: Long?,
        row: SessionLog,
        sessionMaxMillisBetweenEvents: Long
    ) = lastSessionEventTimestamp == null
            || row.timestamp - lastSessionEventTimestamp > sessionMaxMillisBetweenEvents

    private fun readSessionLogDataset(spark: SparkSession, sessionLogFiles: Set<String>) = spark
        .read()
        .parquet(*sessionLogFiles.toTypedArray())
        .`as`(encoder<SessionLog>())

    private fun readRawLogDataset(spark: SparkSession, rawLogFiles: Set<String>) = spark
        .read()
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .csv(*rawLogFiles.toTypedArray())
        .`as`(encoder<RawLog>())
}

internal const val NA_VALUE = "n/a"

/**
 * Saves Dataset partitioned by year, month, day to directories like $path/processed/year=$year/month=$month/day=$day
 */
private fun <T> Dataset<T>.writeWithPartitions(path: String) = this
    .withColumn("timestamp_ts", col("timestamp").divide(functions.lit(1000)).cast(TimestampType))
    .withColumn("year", format_string("%04d", year(functions.col("timestamp_ts"))))
    .withColumn("month", format_string("%02d", month(functions.col("timestamp_ts"))))
    .withColumn("day", format_string("%02d", dayofmonth(functions.col("timestamp_ts"))))
    .drop("timestamp_ts")
    .write()
    .partitionBy("year", "month", "day")
    .mode(SaveMode.Append)
    .parquet(path)