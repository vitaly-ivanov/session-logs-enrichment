package me.vitaly.etl.generator

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import me.vitaly.etl.model.RawLog
import java.io.FileWriter
import java.nio.file.Path
import kotlin.io.path.*

class CsvMaker(private val csvMapper: CsvMapper) {
    @OptIn(ExperimentalPathApi::class)
    fun writeToFile(rows: List<RawLog>): Path =
        createTempFile(prefix = "raw_logs")
            .also { writeCsvFile(rows, it.absolutePathString()) }

    private inline fun <reified T> writeCsvFile(data: Collection<T>, fileName: String) {
        FileWriter(fileName).use { writer ->
            csvMapper.writer(csvMapper.schemaFor(T::class.java).withHeader())
                .writeValues(writer)
                .writeAll(data)
                .close()
        }
    }
}