val bigQueryVersion = "2.43.0"
val prometheusVersion = "1.13.5"
val kotestVersion = "6.0.0.M1"
val logbackVersion = "1.5.8"

plugins {
    kotlin("jvm") version "2.0.20"
    id("com.github.johnrengelman.shadow") version "8.1.1"
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

fun ktor(name: String) = "io.ktor:ktor-$name:3.0.1"

fun kotest(name: String) = "io.kotest:kotest-$name:$kotestVersion"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    // Ktor Server
    implementation(ktor("server-core"))
    implementation(ktor("server-netty"))
    implementation(ktor("server-metrics-micrometer"))

    // BigQuery
    implementation("com.google.cloud:google-cloud-bigquery:$bigQueryVersion")

    // Kafka
    implementation("org.apache.kafka:kafka-clients:3.8.0")

    // Config.kt
    implementation("com.natpryce:konfig:1.6.10.0")

    // Webserver

    // Målinger
    implementation("io.micrometer:micrometer-registry-prometheus:$prometheusVersion")

    // Logging
    implementation("io.github.microutils:kotlin-logging:3.0.5")
    runtimeOnly("ch.qos.logback:logback-classic:$logbackVersion")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:8.0")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation(kotest("runner-junit5"))
    testImplementation(kotest("assertions-core"))
    testImplementation("io.mockk:mockk:1.13.12")

    constraints {
        implementation("io.netty:netty-codec-http2") {
            version {
                require("4.1.114.Final")
            }
            because("Affected versions < 4.1.101.Final are vulnerable to HTTP/2 Rapid Reset Attack")
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
        environment("NAIS_CLUSTER_NAME", "local")
        environment("GCP_TEAM_PROJECT_ID", "pia")
        environment("BIGQUERY_DATASET_ID", "pia_bigquery_sink_v1_dataset_local")
        environment("KAFKA_CONSUMER_LOOP_DELAY", "1000")
        useJUnitPlatform()
    }
}

kotlin {
    jvmToolchain(17)
}
