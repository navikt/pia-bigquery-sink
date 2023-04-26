import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.8.20"
    id("com.github.johnrengelman.shadow") version "7.1.2"
    application
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

fun ktor(name: String) = "io.ktor:ktor-$name:2.3.0"
fun kotest(name: String) = "io.kotest:kotest-$name:5.5.5"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    // Ktor Server
    implementation(ktor("server-core"))
    implementation(ktor("server-netty"))
    implementation(ktor("server-metrics-micrometer"))

    // BigQuery
    implementation("com.google.cloud:google-cloud-bigquery:2.22.0")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.4.0")

    // Config.kt
    implementation("com.natpryce:konfig:1.6.10.0")

    // Webserver

    // Målinger
    implementation("io.micrometer:micrometer-registry-prometheus:1.10.6")

    // Logging
    implementation("io.github.microutils:kotlin-logging:3.0.5")
    runtimeOnly("ch.qos.logback:logback-classic:1.4.6")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:7.3")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation(kotest("runner-junit5"))
    testImplementation(kotest("assertions-core"))
    testImplementation("io.mockk:mockk:1.13.5")
}

application {
    mainClass.set("no.nav.pia.bigquery.sink.AppKt")
}

tasks.test {
    environment("NAIS_CLUSTER_NAME", "local")
    environment("GCP_TEAM_PROJECT_ID", "pia")
    environment("BIGQUERY_DATASET_ID", "pia_bigquery_sink_v1_dataset_local")
    environment("KAFKA_CONSUMER_LOOP_DELAY", "1000")

    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}
