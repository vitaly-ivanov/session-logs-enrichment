package me.vitaly.etl.jobs

import org.apache.spark.api.java.function.FlatMapGroupsFunction
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.KeyValueGroupedDataset
import org.jetbrains.kotlinx.spark.api.encoder

// should have been in kotlin-spark-api
inline fun <KEY, VALUE, reified R> KeyValueGroupedDataset<KEY, VALUE>.flatMapGroups(
    noinline func: (KEY, Iterator<VALUE>) -> Iterator<R>
): Dataset<R> = flatMapGroups(FlatMapGroupsFunction(func), encoder<R>())
