package no.nav.pia.bigquery.sink.helse

import io.ktor.http.HttpStatusCode
import io.ktor.server.application.call
import io.ktor.server.response.respond
import io.ktor.server.response.respondText
import io.ktor.server.routing.Routing
import io.ktor.server.routing.get


fun Routing.healthChecks() {
    get("internal/isalive") {
        if (HelseMonitor.erFrisk()) {
            call.respondText { "OK" }
        } else {
            call.respond(HttpStatusCode.ServiceUnavailable, "Unhealthy")
        }
    }
    get("internal/isready") {
        if (HelseMonitor.erFrisk()) {
            call.respondText { "OK" }
        } else {
            call.respond(HttpStatusCode.ServiceUnavailable, "Unhealthy")
        }
    }
}
