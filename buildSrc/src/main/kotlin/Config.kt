import org.gradle.api.JavaVersion

object Versions {
    val javaVersion = JavaVersion.VERSION_11

    //Plugins
    const val versionsPlugin = "0.38.0"
    const val shadowJarPlugin = "6.1.0"

    const val sparkSql = "3.0.0"
    const val scalaVersion = "2.12"
    const val typeSafeConfig = "1.4.1"
    const val typeSafeConfigGuice = "0.1.0"
//    const val sparkBaseTest = "${sparkSql}_1.0.0"
    const val kotlinxSpark = "1.0.0-preview1"
    const val kotlinJvm = "11"
    const val kotlin = "1.4.32"
    const val ktlintPlugin = "10.0.0"

    // Libs for testing
    const val junit = "5.7.1"
    const val kotest = "4.4.3"
}

object Plugins {
    const val shadowJar = "com.github.johnrengelman.shadow"
    const val versions = "com.github.ben-manes.versions"
    const val ktlint = "org.jlleitschuh.gradle.ktlint"
}

object Libs {
    const val sparkSql = "org.apache.spark:spark-sql_${Versions.scalaVersion}:${Versions.sparkSql}"
    const val typeSafeConfig = "com.typesafe:config:${Versions.typeSafeConfig}"
    const val typeSafeConfigGuice = "com.github.racc:typesafeconfig-guice:${Versions.typeSafeConfigGuice}"
    const val kotlinxSpark = "org.jetbrains.kotlinx.spark:kotlin-spark-api-3.0.0_${Versions.scalaVersion}" +
            ":${Versions.kotlinxSpark}"
    const val kotlinReflect = "org.jetbrains.kotlin:kotlin-reflect:${Versions.kotlin}"

    // Test libraries
    const val junit = "org.junit.jupiter:junit-jupiter-api:${Versions.junit}"
    const val junitEngine = "org.junit.jupiter:junit-jupiter-engine:${Versions.junit}"
    const val junitParams = "org.junit.jupiter:junit-jupiter-params:${Versions.junit}"
//    const val sparkBaseTest = "com.holdenkarau:spark-testing-base_${Versions.scalaVersion}:${Versions.sparkBaseTest}"
    const val kotest = "io.kotest:kotest-assertions-core-jvm:${Versions.kotest}"
}