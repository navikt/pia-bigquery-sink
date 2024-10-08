package no.nav.pia.bigquery.sink.mertics

import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.routing.Routing
import io.ktor.server.routing.get
import io.micrometer.prometheusmetrics.PrometheusConfig
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry

class Metrics {
    companion object {
        val appMicrometerRegistry = PrometheusMeterRegistry(PrometheusConfig.DEFAULT)
    }
}

fun Routing.metrics() {
    get("/metrics") {
        call.respond(Metrics.appMicrometerRegistry.scrape())
    }
}
