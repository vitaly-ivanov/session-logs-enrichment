package me.vitaly.etl.jobs

import com.holdenkarau.spark.testing.JavaDatasetSuiteBase
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import me.vitaly.etl.jobs.SessionLogsEnrichmentJob.buildSessionId
import me.vitaly.etl.model.RawLog
import me.vitaly.etl.model.SessionLog
import org.apache.spark.sql.SparkSession
import org.jetbrains.kotlinx.spark.api.toDS
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.MethodSource
import java.time.Instant
import java.time.OffsetDateTime
import java.time.ZoneOffset

internal class SessionLogsEnrichmentJobTest : JavaDatasetSuiteBase() {
    @BeforeEach
    fun beforeEach() {
        runBefore()
    }

    @MethodSource("enrich logs source")
    @ParameterizedTest
    fun `enrich logs test`(data: TestData) {
        val spark = SparkSession.builder().config(sc().conf).orCreate
        val actualLogs = SessionLogsEnrichmentJob.enrichLogs(
            spark.toDS(data.rawLogDataset),
            spark.toDS(data.sessionDataset),
            userEvents,
            SESSION_MINUTES_BETWEEN_EVENTS
        ).collectAsList()
        actualLogs shouldContainExactlyInAnyOrder data.expectedLogs
    }

    companion object {
        private const val SESSION_MINUTES_BETWEEN_EVENTS = 5
        private val userEvents = setOf("a", "b", "c")

        private val rawLogs = listOf(
            RawLog("device1", createMillis(2020, 4, 1, 10, 0, 0), "a", "product1"),
            RawLog("device2", createMillis(2020, 4, 1, 10, 1, 0), "a", "product1"),
            RawLog("device1", createMillis(2020, 4, 1, 10, 0, 30), "b", "product2"),
            RawLog("device1", createMillis(2020, 4, 1, 10, 3, 0), "d", "product1"),
            RawLog("device2", createMillis(2020, 4, 1, 10, 6, 1), "a", "product1"),
            RawLog("device3", createMillis(2020, 4, 1, 10, 0, 0), "e", "product3"),
            RawLog("device1", createMillis(2020, 4, 2, 10, 0, 30), "a", "product1"),
        )

        private val technicalEventsBetweenUserOnesRasLogs = listOf(
            RawLog("device1", createMillis(2020, 4, 1, 10, 0, 0), "a", "product1"),
            RawLog("device1", createMillis(2020, 4, 1, 10, 4, 0), "j", "product1"),
            RawLog("device1", createMillis(2020, 4, 1, 10, 8, 30), "a", "product1"),
        )

        @JvmStatic
        @AfterAll
        fun afterEach() {
            runAfterClass()
        }

        @JvmStatic
        private fun `enrich logs source`() = listOf(
            TestData(
                "empty",
                listOf(),
                listOf(),
                listOf(),
            ),
            TestData(
                "only new data",
                rawLogs,
                listOf(),
                listOf(
                    rawLogs[0].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                    rawLogs[1].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                    rawLogs[2].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                    rawLogs[3].let {
                        it.toSessionLog(
                            buildSessionId(rawLogs[0].device_id, rawLogs[0].product, rawLogs[0].timestamp)
                        )
                    },
                    rawLogs[4].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                    rawLogs[5].let { it.toSessionLog(NA_VALUE) },
                    rawLogs[6].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                )
            ),
            TestData(
                "new and existing data",
                rawLogs,
                listOf(
                    SessionLog("device1", createMillis(2020, 4, 1, 9, 56, 0), "g", "product1", NA_VALUE),
                    SessionLog(
                        "device2", createMillis(2020, 4, 1, 9, 59, 0), "d", "product1",
                        buildSessionId("device0", "product0", Instant.EPOCH.toEpochMilli())
                    )
                ),
                listOf(
                    rawLogs[0].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                    rawLogs[1].let {
                        it.toSessionLog(
                            buildSessionId("device0", "product0", Instant.EPOCH.toEpochMilli())
                        )
                    },
                    rawLogs[2].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                    rawLogs[3].let {
                        it.toSessionLog(
                            buildSessionId(rawLogs[0].device_id, rawLogs[0].product, rawLogs[0].timestamp)
                        )
                    },
                    rawLogs[4].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                    rawLogs[5].let { it.toSessionLog(NA_VALUE) },
                    rawLogs[6].let { it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp)) },
                )
            ),
            TestData(
                "technical events between user ones",
                technicalEventsBetweenUserOnesRasLogs,
                listOf(),
                listOf(
                    technicalEventsBetweenUserOnesRasLogs[0].let {
                        it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp))
                    },
                    technicalEventsBetweenUserOnesRasLogs[1].let {
                        it.toSessionLog(
                            buildSessionId(
                                technicalEventsBetweenUserOnesRasLogs[0].device_id,
                                technicalEventsBetweenUserOnesRasLogs[0].product,
                                technicalEventsBetweenUserOnesRasLogs[0].timestamp
                            )
                        )
                    },
                    technicalEventsBetweenUserOnesRasLogs[2].let {
                        it.toSessionLog(buildSessionId(it.device_id, it.product, it.timestamp))
                    }
                )
            ),
        )
    }
}

internal data class TestData(
    val testCaseName: String,
    val rawLogDataset: List<RawLog>,
    val sessionDataset: List<SessionLog>,
    val expectedLogs: List<SessionLog>
)

private fun createMillis(year: Int, month: Int, day: Int, hour: Int, minute: Int, second: Int) = OffsetDateTime.of(
    year, month, day, hour, minute, second, 0, ZoneOffset.UTC
)
    .toEpochSecond() * 1000
