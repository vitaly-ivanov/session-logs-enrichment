package me.vitaly.etl.local

import me.vitaly.etl.jobs.SessionLogsEnrichmentJob
import org.apache.spark.sql.SparkSession


fun main() {
    SessionLogsEnrichmentJob.run(
        spark = SparkSession
            .builder()
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .master("local[2]")
            .appName(SessionLogsEnrichmentJob::class.simpleName)
            .orCreate,
        rawLogFiles = setOf("data/raw/raw_logs.csv"),
        sessionLogFiles = setOf("data/processed/session_logs.csv"),
        resultPath = "data/processed",
        sessionMaxMinutesBetweenEvents = 5,
        userEvents = setOf("a", "b", "c")
    )
}
