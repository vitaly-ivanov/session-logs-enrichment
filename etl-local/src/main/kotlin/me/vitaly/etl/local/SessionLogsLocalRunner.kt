package me.vitaly.etl.local

import com.typesafe.config.ConfigFactory
import me.vitaly.etl.jobs.SessionLogsEnrichmentJob
import me.vitaly.etl.runners.SessionLogsEnrichmentJobRunner
import org.apache.spark.sql.SparkSession
import java.time.LocalDate

private val DATE = LocalDate.of(2021, 4, 11)

fun main() {
    val config = ConfigFactory.load().getConfig("sessionLogsJob")
    SessionLogsEnrichmentJobRunner(
        spark = SparkSession
            .builder()
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .master("local[2]")
            .appName(SessionLogsEnrichmentJob::class.simpleName)
            .orCreate,
        rawLogPath = config.getString("rawLogPath"),
        sessionLogPath = config.getString("sessionLogPath"),
        sessionMaxMinutesBetweenEvents = config.getInt("sessionMaxMinutesBetweenEvents"),
        sessionLogDaysAnalyze = config.getInt("sessionLogDaysAnalyze"),
        userEvents = config.getStringList("userEvents").toSet()
    ).runJob(DATE)
}
