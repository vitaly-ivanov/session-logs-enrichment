import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar

plugins {
    kotlin("jvm")
    id(Plugins.shadowJar) version Versions.shadowJarPlugin
    `java-library`
}

group = "ru.vitaly.etl"

java {
    sourceCompatibility = Versions.javaVersion
    targetCompatibility = Versions.javaVersion
}

tasks.getByName<ShadowJar>("shadowJar") {
    isZip64 = true
    // For spark jars, see https://imperceptiblethoughts.com/shadow/configuration/merging/
    mergeServiceFiles()
    archiveFileName.set("etl.jar")
}

tasks {
    build {
        dependsOn(shadowJar)
    }
}

dependencies {
    compileOnly(Libs.sparkSql)

    implementation(Libs.kotlinxSpark)
    implementation(Libs.typeSafeConfig)
    implementation(Libs.typeSafeConfigGuice)
    implementation(Libs.kotlinReflect)

    testImplementation(Libs.junit)
    testImplementation(Libs.junitEngine)
    testImplementation(Libs.junitParams)
//    testImplementation(Libs.sparkBaseTest)
    testImplementation(Libs.sparkSql)
    testImplementation(Libs.kotest)
}

tasks.test {
    useJUnitPlatform()
}

tasks {
    compileKotlin {
        kotlinOptions.jvmTarget = Versions.kotlinJvm
    }
    compileTestKotlin {
        kotlinOptions.jvmTarget = Versions.kotlinJvm
    }
}
