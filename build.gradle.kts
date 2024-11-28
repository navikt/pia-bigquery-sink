val ktorVersion = "3.0.1"
val iaFellesVersion = "1.9.2"
val prometheusVersion = "1.13.6"
val bigQueryVersion = "2.43.3"
val kafkaVersion = "3.8.1"
val mockkVersion = "1.13.13"
val kotestVerstion = "6.0.0.M1"
val testcontainersVersion = "1.20.3"
val logbackVersion = "1.5.12"

plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

group = "no.nav"

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

dependencies {
    implementation(kotlin("stdlib"))
    implementation(kotlin("reflect"))

    // Felles definisjoner for IA-domenet
    implementation("com.github.navikt:ia-felles:$iaFellesVersion")

    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.6.1")
    testImplementation("org.wiremock:wiremock-standalone:3.9.2")

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
    implementation("net.logstash.logback:logstash-logback-encoder:8.0")

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

    testImplementation("org.testcontainers:testcontainers:$testcontainersVersion")
    testImplementation("org.testcontainers:kafka:$testcontainersVersion")
    testImplementation("org.testcontainers:gcloud:$testcontainersVersion")

    constraints {
        implementation("io.netty:netty-codec-http2") {
            version {
                require("4.1.114.Final")
            }
            because("From Ktor version: 2.3.5 -> io.netty:netty-codec-http2 vulnerable to HTTP/2 Rapid Reset Attack")
        }
        testImplementation("org.apache.commons:commons-compress") {
            version {
                require("1.27.1")
            }
            because("testcontainers har sårbar versjon")
        }
        testImplementation("commons-io:commons-io") {
            version {
                require("2.17.0")
            }
            because("testcontainers har sårbar versjon")
        }
    }
}

tasks {
    shadowJar {
        manifest {
            attributes("Main-Class" to "no.nav.pia.bigquery.sink.AppKt")
        }
    }

    test {
        dependsOn(shadowJar)
        environment("NAIS_CLUSTER_NAME", "local") // TODO: usikker på om disse trengs om det settes i testcontainers
        environment("GCP_TEAM_PROJECT_ID", "pia")
        environment("BIGQUERY_DATASET_ID", "pia_bigquery_sink_v1_dataset_local")
        environment("KAFKA_CONSUMER_LOOP_DELAY", "1000")
        useJUnitPlatform()
    }
}

kotlin {
    jvmToolchain(17)
}
