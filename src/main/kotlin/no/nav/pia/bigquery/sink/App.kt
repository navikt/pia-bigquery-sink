package no.nav.pia.bigquery.sink

import io.ktor.server.application.Application
import io.ktor.server.engine.addShutdownHook
import io.ktor.server.engine.embeddedServer
import io.ktor.server.engine.stop
import io.ktor.server.netty.Netty
import io.ktor.server.routing.routing
import no.nav.pia.bigquery.sink.datadefenisjoner.schemaRegistry
import no.nav.pia.bigquery.sink.helse.HelseMonitor
import no.nav.pia.bigquery.sink.helse.healthChecks
import no.nav.pia.bigquery.sink.konfigurasjon.Clusters
import no.nav.pia.bigquery.sink.konfigurasjon.Kafka
import no.nav.pia.bigquery.sink.konfigurasjon.Miljø
import no.nav.pia.bigquery.sink.mertics.metrics
import java.util.concurrent.TimeUnit

fun main() {
    val bigQueryClient: BigQueryClient = when (Miljø.cluster) {
        Clusters.LOKAL.clusterId -> LocalBigQueryClient()
        else -> DefaultBigQueryClient(Miljø.team_project_id)
    }
    val bigQueryService: BigQueryService = BigQueryService(
        Miljø.team_project_id,
        bigQueryClient,
    ).apply {
        migrate(schemaRegistry)
    }

    val kafkaKonfig = Kafka()
    PiaKafkaLytter().apply {
        create(
            topic = kafkaKonfig.iaSakStatistikkTopic,
            kafkaKonfigurasjon = kafkaKonfig,
            bigQueryHendelseMottak = BigQueryHendelseMottak(bigQueryService),
        )
        run()
    }.also { HelseMonitor.leggTilHelsesjekk(it) }

    PiaKafkaLytter().apply {
        create(
            topic = kafkaKonfig.iaSakLeveranseTopic,
            kafkaKonfigurasjon = kafkaKonfig,
            bigQueryHendelseMottak = BigQueryHendelseMottak(bigQueryService),
        )
        run()
    }.also { HelseMonitor.leggTilHelsesjekk(it) }

    embeddedServer(Netty, port = 8080, module = Application::myApplicationModule).also {
        // https://doc.nais.io/nais-application/good-practices/#handles-termination-gracefully
        it.addShutdownHook {
            it.stop(3, 5, TimeUnit.SECONDS)
        }
    }.start(wait = true)
}

fun Application.myApplicationModule() {
    routing {
        healthChecks()
        metrics()
    }
}
