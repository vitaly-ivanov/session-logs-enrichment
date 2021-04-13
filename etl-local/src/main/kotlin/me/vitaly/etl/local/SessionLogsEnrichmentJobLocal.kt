package me.vitaly.etl.local

import me.vitaly.etl.jobs.SessionLogsEnrichmentJob
import me.vitaly.etl.runners.SessionLogsEnrichmentJobRunner
import org.apache.spark.sql.SparkSession
import java.time.LocalDate


fun main() {
    SessionLogsEnrichmentJobRunner(
        spark = SparkSession
            .builder()
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .master("local[2]")
            .appName(SessionLogsEnrichmentJob::class.simpleName)
            .orCreate,
        rawLogPath = "data/raw/",
        sessionLogPath = "data/processed/",
        sessionMaxMinutesBetweenEvents = 5,
        sessionLogDaysAnalyze = 6,
        userEvents = setOf("a", "b", "c")
    ).runJob(LocalDate.of(2021, 4, 11))
}
