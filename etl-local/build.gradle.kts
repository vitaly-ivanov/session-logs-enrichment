plugins {
    kotlin("jvm")
}

group = "ru.vitaly.etl"

java {
    sourceCompatibility = Versions.javaVersion
    targetCompatibility = Versions.javaVersion
}

dependencies {
    implementation(Libs.sparkSql)
    implementation(project(":etl"))
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
