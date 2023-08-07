plugins {
    kotlin("jvm") version "1.9.0"
    id("com.github.johnrengelman.shadow") version "8.1.1"
    application
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

fun ktor(name: String) = "io.ktor:ktor-$name:2.3.3"
fun kotest(name: String) = "io.kotest:kotest-$name:5.6.2"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    // Ktor Server
    implementation(ktor("server-core"))
    implementation(ktor("server-netty"))
    implementation(ktor("server-metrics-micrometer"))

    // BigQuery
    implementation("com.google.cloud:google-cloud-bigquery:2.31.0")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.4.1")

    // Config.kt
    implementation("com.natpryce:konfig:1.6.10.0")

    // Webserver

    // Målinger
    implementation("io.micrometer:micrometer-registry-prometheus:1.11.0")

    // Logging
    implementation("io.github.microutils:kotlin-logging:3.0.5")
    runtimeOnly("ch.qos.logback:logback-classic:1.4.8")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:7.4")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation(kotest("runner-junit5"))
    testImplementation(kotest("assertions-core"))
    testImplementation("io.mockk:mockk:1.13.5")
}

/*
    Guava ødela en release som blir brukt av google-cloud-bigquery.
    Workaround under kan fjernes når de har oppdatert

    For info: https://github.com/google/guava/releases/tag/v32.1.0

    - Start workaround -
*/
configurations.all {
    resolutionStrategy.capabilitiesResolution.withCapability("com.google.guava:listenablefuture") {
        select("com.google.guava:guava:0")
    }
}
/* - Slutt workaround - */

application {
    mainClass.set("no.nav.pia.bigquery.sink.AppKt")
}

kotlin {
    jvmToolchain(17)
}

tasks.test {
    environment("NAIS_CLUSTER_NAME", "local")
    environment("GCP_TEAM_PROJECT_ID", "pia")
    environment("BIGQUERY_DATASET_ID", "pia_bigquery_sink_v1_dataset_local")
    environment("KAFKA_CONSUMER_LOOP_DELAY", "1000")

    useJUnitPlatform()
}
