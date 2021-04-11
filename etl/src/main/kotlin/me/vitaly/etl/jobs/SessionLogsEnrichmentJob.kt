package me.vitaly.etl.jobs

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
        analyzeDays: Int,
        sessionMaxMinutesBetweenEvents: Int,
        userEvents: Set<String>,
    ) {
        val rawLogDataset = readRawLogDataset(spark, rawLogFiles)
        val sessionDataset = readSessionLogDataset(spark, sessionLogFiles)
        rawLogDataset
            .map { it.toSessionLog() }
            .union(sessionDataset)
            .groupByKey { c((it.product), it.device_id) }
            .flatMapGroups { _, iterator -> fillSession(
                iterator,
                userEvents,
                sessionMaxMinutesBetweenEvents
            ) }
            .writeWithPartitions(resultPath)
    }

    private fun fillSession(
        iterator: Iterator<SessionLog>,
        userEvents: Set<String>,
        sessionMaxMinutesBetweenEvents: Int
    ): Iterator<SessionLog> {
        var sessionId: String? = null
        var lastSessionEventTimestamp: Long? = null
        return iterator.asSequence()
            .sortedBy { it.timestamp }
            .mapNotNull { row ->
                when {
                    isAlreadyProcessedLog(row) -> {
                        sessionId = row.session_id
                        lastSessionEventTimestamp = row.timestamp
                        // session logs required only for calculations, they should not be stored
                        return@mapNotNull null
                    }
                    isNeededToStartSession(lastSessionEventTimestamp, row, sessionMaxMinutesBetweenEvents) ->
                        if (row.event in userEvents) {
                            sessionId = "${row.device_id}#${row.product}#${row.timestamp}"
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
                    session_id = sessionId
                )
            }.iterator()
    }

    private fun isAlreadyProcessedLog(row: SessionLog) = row.session_id != null

    private fun isNeededToStartSession(
        lastSessionEventTimestamp: Long?,
        row: SessionLog,
        sessionMaxMinutesBetweenEvents: Int
    ) = lastSessionEventTimestamp == null
            || TimeUnit.MILLISECONDS.toMinutes(row.timestamp - lastSessionEventTimestamp) > sessionMaxMinutesBetweenEvents

    private fun readSessionLogDataset(spark: SparkSession, sessionLogFiles: Set<String>) = spark
        .read()
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .csv(*sessionLogFiles.toTypedArray())
        .`as`(encoder<SessionLog>())

    private fun readRawLogDataset(spark: SparkSession, rawLogFiles: Set<String>) = spark
        .read()
        .option("inferSchema", "true")
        .option("header", "true")
        .option("delimiter", ",")
        .csv(*rawLogFiles.toTypedArray())
        .`as`(encoder<RawLog>())

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
}
