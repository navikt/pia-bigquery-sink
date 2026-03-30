val bigQueryVersion = "2.62.0"
val kafkaVersion = "4.2.0"
val kotestVerstion = "6.1.7"
val ktorVersion = "3.4.1"
val logbackEncoderVersion = "9.0"
val logbackVersion = "1.5.32"
val mockkVersion = "1.14.9"
val prometheusVersion = "1.16.4"
val testcontainersVersion = "2.0.4"
val wiremockVersion = "3.13.2"

plugins {
    kotlin("jvm") version "2.3.20"
    kotlin("plugin.serialization") version "2.3.20"
    id("application")
}

group = "no.nav"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))

    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.7.1-0.6.x-compat")

    // Ktor Server
    implementation("io.ktor:ktor-server-core:$ktorVersion")
    implementation("io.ktor:ktor-server-netty:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-core-jvm:$ktorVersion")
    implementation("io.ktor:ktor-serialization-kotlinx-json-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-auth-jvm:$ktorVersion")
    implementation("io.ktor:ktor-client-core:$ktorVersion")
    implementation("io.ktor:ktor-client-cio:$ktorVersion")
    implementation("io.ktor:ktor-client-content-negotiation-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-netty-jvm:$ktorVersion")
    implementation("io.ktor:ktor-server-status-pages-jvm:$ktorVersion")

    implementation("ch.qos.logback:logback-classic:$logbackVersion")
    implementation("net.logstash.logback:logstash-logback-encoder:$logbackEncoderVersion")

    // Metrics
    implementation("io.ktor:ktor-server-metrics-micrometer-jvm:$ktorVersion")
    implementation("io.micrometer:micrometer-registry-prometheus:$prometheusVersion")

    // BigQuery
    implementation("com.google.cloud:google-cloud-bigquery:$bigQueryVersion")

    // Kafka
    implementation("at.yawk.lz4:lz4-java:1.10.4")
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion") {
        // "Fikser CVE-2025-12183 - lz4-java >1.8.1 har sårbar versjon (transitive dependency fra kafka-clients:4.1.0)"
        exclude("org.lz4", "lz4-java")
    }

    // Logging
    implementation("io.github.microutils:kotlin-logging:3.0.5")
    // TODO: Flyttet til io.github.oshai » kotlin-logging, men trenger vi bruke denne engang?

    // Testing
    testImplementation("org.jetbrains.kotlin:kotlin-test")
    testImplementation("io.kotest:kotest-assertions-core:$kotestVerstion")
    testImplementation("io.kotest:kotest-assertions-json:$kotestVerstion")
    testImplementation("io.mockk:mockk:$mockkVersion")
    testImplementation("org.wiremock:wiremock-standalone:$wiremockVersion")
    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-kafka:$testcontainersVersion")
    testImplementation("org.testcontainers:testcontainers-gcloud:$testcontainersVersion")

    constraints {
        implementation("com.fasterxml.jackson.core:jackson-core") {
            version { require("2.21.1") }
            because("versjoner < 2.21.1 har sårbarhet. inkludert i ktor-server-auth:3.4.0")
        }
        implementation("tools.jackson.core:jackson-core") {
            version { require("3.1.0") }
            because("versjoner < 3.1.0 har sårbarhet. inkludert i logstash-logback-encoder:9.0")
        }
    }
}

tasks {
    test {
        dependsOn(installDist)
        environment("NAIS_CLUSTER_NAME", "local")
        environment("GCP_TEAM_PROJECT_ID", "pia")
        environment("BIGQUERY_DATASET_ID", "pia_bigquery_sink_v1_dataset_local")
        environment("KAFKA_CONSUMER_LOOP_DELAY", "1000")
        useJUnitPlatform()
    }
}

kotlin {
    jvmToolchain(21)
}
