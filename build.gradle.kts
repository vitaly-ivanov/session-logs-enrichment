import com.github.benmanes.gradle.versions.updates.DependencyUpdatesTask
// Top-level build file where you can add configuration options common to all sub-projects/modules.

plugins {
    id(Plugins.versions) version Versions.versionsPlugin
    kotlin("jvm") version Versions.kotlin
    id(Plugins.ktlint) version Versions.ktlintPlugin
}

allprojects {
    repositories {
        mavenCentral()
    }
}

fun isNonStable(version: String): Boolean {
    val stableKeyword = listOf("RELEASE", "FINAL", "GA").any { version.toUpperCase().contains(it) }
    val regex = "^[0-9,.v-]+(-r)?$".toRegex()
    val isStable = stableKeyword || regex.matches(version)
    return isStable.not()
}

// You can see all outdated dependencies using the command `./gradlew dependencyUpdates`
tasks {
    named<DependencyUpdatesTask>("dependencyUpdates") {
        resolutionStrategy {
            componentSelection {
                all {
                    if (isNonStable(candidate.version) && !isNonStable(currentVersion)) {
                        reject("Release candidate")
                    }
                }
            }
        }
        checkForGradleUpdate = true
        outputFormatter = "json"
        outputDir = "build/dependencyUpdates"
        reportfileName = "report"
    }
}
