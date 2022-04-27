import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

plugins {
    kotlin("jvm") version "1.6.21"
    application
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

fun ktor(name: String) = "io.ktor:ktor-$name:2.0.0"
fun kotest(name: String) = "io.kotest:kotest-$name:5.1.0"

dependencies {
    implementation(kotlin("stdlib-jdk8"))
    implementation(kotlin("reflect"))

    // Ktor Server
    implementation(ktor("server-core"))
    implementation(ktor("server-cio"))
    implementation(ktor("server-metrics-micrometer"))

    implementation("com.github.navikt:rapids-and-rivers:2022.04.21-09.34.08966130226f") {
        exclude(group = "ch.qos.logback")
    }

    implementation("com.natpryce:konfig:1.6.10.0")
    implementation("com.google.cloud:google-cloud-bigquery:2.10.10")
    implementation("io.micrometer:micrometer-registry-prometheus:1.8.5")
    implementation("org.reflections:reflections:0.10.2")

    // Logging
    implementation("io.github.microutils:kotlin-logging:2.1.21")
    runtimeOnly("ch.qos.logback:logback-classic:1.2.11")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:7.1.1")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation(kotest("runner-junit5"))
    testImplementation(kotest("assertions-core"))
    testImplementation("io.mockk:mockk:1.12.3")
}

application {
    mainClass.set("no.nav.hjelpemidler.bigquery.sink.AppKt")
}

tasks.test {
    useJUnitPlatform()
}

tasks.withType<KotlinCompile> {
    kotlinOptions.jvmTarget = "17"
}

tasks.withType<Jar> {
    duplicatesStrategy = DuplicatesStrategy.EXCLUDE
    manifest {
        attributes["Main-Class"] = application.mainClass
    }
    from(
        configurations.runtimeClasspath.get().map {
            if (it.isDirectory) it else zipTree(it)
        }
    )
}
