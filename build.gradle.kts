import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.7.10"
    id("com.github.johnrengelman.shadow") version "7.1.2"
    application
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

fun ktor(name: String) = "io.ktor:ktor-$name:2.1.0"
fun kotest(name: String) = "io.kotest:kotest-$name:5.3.2"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    // Ktor Server
    implementation(ktor("server-core"))
    implementation(ktor("server-cio"))
    implementation(ktor("server-metrics-micrometer"))

    // BigQuery
    implementation("com.google.cloud:google-cloud-bigquery:2.22.0")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.3.1")

    // Config.kt
    implementation("com.natpryce:konfig:1.6.10.0")

    // Målinger
    implementation("io.micrometer:micrometer-registry-prometheus:1.9.3")

    // Logging
    implementation("io.github.microutils:kotlin-logging:2.1.23")
    runtimeOnly("ch.qos.logback:logback-classic:1.4.0")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:7.2")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation(kotest("runner-junit5"))
    testImplementation(kotest("assertions-core"))
    testImplementation("io.mockk:mockk:1.12.7")
}

application {
    mainClass.set("no.nav.pia.bigquery.sink.AppKt")
}

tasks.test {
    environment("NAIS_CLUSTER_NAME", "local")
    environment("TEAM_PROJECT_ID", "pia")
    environment("DATASET_ID", "pia_bigquery_sink_v1_dataset_local")

    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}
