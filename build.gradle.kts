plugins {
    java
}

repositories {
    mavenCentral()
}

// Run against another release with -PdebeziumVersion=3.7.1.Final
val debeziumVersion = providers.gradleProperty("debeziumVersion").getOrElse("3.6.3.Final")

dependencies {
    testImplementation("io.debezium:debezium-embedded:$debeziumVersion")
    testImplementation("io.debezium:debezium-connector-postgres:$debeziumVersion")

    testImplementation("io.zonky.test:embedded-postgres:2.2.2")
    testImplementation(enforcedPlatform("io.zonky.test.postgres:embedded-postgres-binaries-bom:17.11.0"))
    testRuntimeOnly("io.zonky.test.postgres:embedded-postgres-binaries-darwin-arm64v8")
    testRuntimeOnly("io.zonky.test.postgres:embedded-postgres-binaries-darwin-amd64")
    testRuntimeOnly("io.zonky.test.postgres:embedded-postgres-binaries-linux-amd64")
    testRuntimeOnly("io.zonky.test.postgres:embedded-postgres-binaries-linux-arm64v8")

    testImplementation(platform("org.junit:junit-bom:6.1.3"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testRuntimeOnly("ch.qos.logback:logback-classic:1.5.18")
}

tasks.test {
    useJUnitPlatform()
    // The connector's TRACE log goes to build/debezium-<version>.log (logback-test.xml).
    systemProperty("debezium.version", debeziumVersion)
    systemProperty("log.dir", layout.buildDirectory.get().asFile.path)
    outputs.upToDateWhen { false }
    testLogging {
        events("passed", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = false
    }
}
