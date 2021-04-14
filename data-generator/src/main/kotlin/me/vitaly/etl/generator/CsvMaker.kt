package me.vitaly.etl.generator

import com.fasterxml.jackson.dataformat.csv.CsvMapper
import java.io.FileWriter
import java.nio.file.Path
import kotlin.io.path.ExperimentalPathApi
import kotlin.io.path.absolutePathString
import kotlin.io.path.createTempFile

class CsvMaker(val csvMapper: CsvMapper) {
    @OptIn(ExperimentalPathApi::class)
    inline fun <reified T> writeToFile(rows: List<T>): Path =
        createTempFile(prefix = "raw_logs")
            .also { writeCsvFile(rows, it.absolutePathString()) }

    inline fun <reified T> writeCsvFile(data: Collection<T>, fileName: String) {
        FileWriter(fileName).use { writer ->
            csvMapper.writer(csvMapper.schemaFor(T::class.java).withHeader())
                .writeValues(writer)
                .writeAll(data)
                .close()
        }
    }
}
