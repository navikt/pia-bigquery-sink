import org.jetbrains.kotlin.gradle.tasks.KotlinCompile

val jacksonVersion = "2.13.1"

plugins {
    kotlin("jvm") version "1.6.10"
    application
}

repositories {
    mavenCentral()
    maven("https://jitpack.io")
}

fun ktor(name: String) = "io.ktor:ktor-$name:1.6.7"
fun kotest(name: String) = "io.kotest:kotest-$name:5.1.0"

dependencies {
    implementation(kotlin("stdlib"))

    // Ktor Server
    implementation(ktor("server-core"))
    implementation(ktor("server-netty"))
    implementation(ktor("metrics-micrometer"))

    implementation("com.natpryce:konfig:1.6.10.0")
    implementation("com.github.navikt:rapids-and-rivers:2022.02.02-14.07.dc18de6a253c")
    implementation("com.google.cloud:google-cloud-bigquery:2.9.0")
    implementation("io.micrometer:micrometer-registry-prometheus:1.8.2")
    implementation("org.reflections:reflections:0.10.2")

    // Logging
    implementation("io.github.microutils:kotlin-logging:2.1.21")
    runtimeOnly("org.slf4j:slf4j-api:2.0.0-alpha6")
    runtimeOnly("ch.qos.logback:logback-classic:1.3.0-alpha13")
    runtimeOnly("net.logstash.logback:logstash-logback-encoder:7.0.1")

    // Testing
    testImplementation(kotlin("test"))
    testImplementation(ktor("server-test-host"))
    testImplementation(kotest("runner-junit5"))
    testImplementation(kotest("assertions-core"))
    testImplementation("io.mockk:mockk:1.12.2")
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

