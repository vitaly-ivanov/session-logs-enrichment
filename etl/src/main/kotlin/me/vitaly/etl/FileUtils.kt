package me.vitaly.etl

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.time.LocalDate

const val PROCESSED_SUFFIX = ".processed"

fun getDatePartitionedFiles(
    fileSystem: FileSystem,
    basePath: String,
    date: LocalDate,
    fileFilter: (String) -> Boolean = { true }
): Set<String> =
    fileSystem.listStatus(Path(basePath))
        .asSequence()
        .filter { it.isDirectory }
        .filter { it.path.name == "year=${date.year.toString().padStart(4, '0')}" }
        .flatMap { fileSystem.listStatus(it.path).toList() }
        .filter { it.isDirectory }
        .filter { it.path.name == "month=${date.monthValue.toString().padStart(2, '0')}" }
        .flatMap { fileSystem.listStatus(it.path).toList() }
        .filter { it.isDirectory }
        .filter { it.path.name == "day=${date.dayOfMonth.toString().padStart(2, '0')}" }
        .flatMap { fileSystem.listStatus(it.path).toList() }
        .filter { it.isFile }
        .map { it.path.toString() }
        .filter(fileFilter)
        .toSet()