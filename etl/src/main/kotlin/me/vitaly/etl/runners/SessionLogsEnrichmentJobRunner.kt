package me.vitaly.etl.runners

import me.vitaly.etl.PROCESSED_SUFFIX
import me.vitaly.etl.getDatePartitionedFiles
import me.vitaly.etl.jobs.SessionLogsEnrichmentJob
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.jetbrains.kotlinx.spark.api.SparkSession
import java.time.LocalDate
import java.util.*

private val logger = mu.KotlinLogging.logger {}

class SessionLogsEnrichmentJobRunner(
    private val spark: SparkSession,
    private val rawLogPath: String,
    private val sessionLogPath: String,
    private val sessionLogDaysAnalyze: Int,
    private val sessionMaxMinutesBetweenEvents: Int,
    private val userEvents: Set<String>
) {
    fun runJob(date: LocalDate) {
        // ensure UTC timezone
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
        val fileSystem = FileSystem.get(spark.sparkContext().hadoopConfiguration())
        val sessionLogFiles = calculateSessionFiles(fileSystem, date)
        val rawLogFiles = calculateUnprocessedRawFiles(fileSystem, date)
        if (rawLogFiles.isEmpty()) {
            logger.debug { "No unprocessed files found. Nothing to do." }
            return
        }
        SessionLogsEnrichmentJob.run(
            spark = spark,
            rawLogFiles = rawLogFiles,
            sessionLogFiles = sessionLogFiles,
            resultPath = sessionLogPath,
            sessionMaxMinutesBetweenEvents = sessionMaxMinutesBetweenEvents,
            userEvents = userEvents
        )

        rawLogFiles
            .map { "$it$PROCESSED_SUFFIX" }
            .forEach {
                fileSystem.create(Path(it)).close()
                logger.debug { "File $it marked as processed." }
            }
    }

    private fun calculateUnprocessedRawFiles(
        fileSystem: FileSystem,
        date: LocalDate
    ) = getDatePartitionedFiles(fileSystem, rawLogPath, date) { fileName ->
        fileName.endsWith(".csv")
    }.minus(
        getDatePartitionedFiles(fileSystem, rawLogPath, date) { fileName ->
            fileName.endsWith(PROCESSED_SUFFIX)
        }.map { it.removeSuffix(PROCESSED_SUFFIX) }
    )

    private fun calculateSessionFiles(
        fileSystem: FileSystem,
        date: LocalDate
    ) = (1L..sessionLogDaysAnalyze).flatMap {
        getDatePartitionedFiles(fileSystem, sessionLogPath, date.minusDays(it)) { fileName ->
            fileName.endsWith(".parquet")
        }
    }.toSet()
}