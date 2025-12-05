val bigQueryVersion = "2.56.0"
val kafkaVersion = "4.1.0"
val kotestVerstion = "6.0.4"
val ktorVersion = "3.3.1"
val logbackEncoderVersion = "9.0"
val logbackVersion = "1.5.20"
val mockkVersion = "1.14.6"
val prometheusVersion = "1.15.5"
val testcontainersVersion = "2.0.1"
val wiremockVersion = "3.13.1"

plugins {
    kotlin("jvm") version "2.2.21"
    kotlin("plugin.serialization") version "2.2.21"
    id("application")
}

group = "no.nav"

repositories {
    mavenCentral()
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))

    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.6.2")

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
    implementation("org.apache.kafka:kafka-clients:$kafkaVersion")

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
        implementation("org.lz4:lz4-java") {
            modules {
                module("org.lz4:lz4-java") {
                    replacedBy("at.yawk.lz4:lz4-java", "Fork of the original unmaintained lz4-java library that fixes a CVE")
                }
            }
            version {
                require("1.8.1")
            }
            because(
                "Fikser CVE-2025-12183 - lz4-java 1.8.0 har sårbar versjon (transitive dependency fra kafka-clients:4.1.0)",
            )
        }
        implementation("net.minidev:json-smart") {
            version {
                require("2.5.2")
            }
            because(
                "versjoner < 2.5.2 har diverse sårbarheter. Inkludert i kotest 6.0.0.M4",
            )
        }
    }
}

tasks {
    test {
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
