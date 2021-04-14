package me.vitaly.etl.generator

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import me.vitaly.etl.model.RawLog
import java.nio.file.Files
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.copyTo
import kotlin.io.path.absolutePathString
import kotlin.io.path.name

private const val ROWS_NUMBER = 1_000_000
private val PRODUCTS_NUMBERS = 1..50
private val DEVICES_NUMBERS = 1..100_000
private val EVENTS = 'a'..'k'
private val SECONDS_BETWEEN_EVENTS = 1..100

// events start date
private val DATE = LocalDate.of(2021, 4, 13)
private var timestamp = DATE.atStartOfDay().toEpochSecond(ZoneOffset.UTC) * 1000

private val logger = mu.KotlinLogging.logger {}

fun main() {
    DataGenerator(CsvMaker(CsvMapper())).generate()
}

class DataGenerator(private val csvMaker: CsvMaker) {
    fun generate() = (1..ROWS_NUMBER)
        .map {
            RawLog(
                device_id = "device${DEVICES_NUMBERS.random()}",
                timestamp = timestamp.also { timestamp += SECONDS_BETWEEN_EVENTS.random() },
                product = "product${PRODUCTS_NUMBERS.random()}",
                event = "${EVENTS.random()}",
            )
        }.let { writeToFile(it) }

    @OptIn(ExperimentalPathApi::class)
    private fun writeToFile(rawLogs: List<RawLog>) {
        val tomorrow = DATE.plusDays(1)
        csvMaker.writeToFile(rawLogs)
            .let { tmpFile ->
                val dataDirectory = Paths.get(
                    "data/raw/" +
                        "year=${tomorrow.year.toString().padStart(4, '0')}/" +
                        "month=${tomorrow.monthValue.toString().padStart(2, '0')}/" +
                        "day=${tomorrow.dayOfMonth.toString().padStart(2, '0')}/"
                )
                Files.createDirectories(dataDirectory)
                tmpFile.copyTo(Paths.get(dataDirectory.absolutePathString() + "/" + tmpFile.name + ".csv"))
                logger.debug { "Data generated to $dataDirectory" }
            }
    }
}
