package no.nav.pia.bigquery.sink

import io.ktor.server.application.Application
import io.ktor.server.engine.addShutdownHook
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import io.ktor.server.routing.routing
import no.nav.pia.bigquery.sink.datadefenisjoner.schemaRegistry
import no.nav.pia.bigquery.sink.helse.HelseMonitor
import no.nav.pia.bigquery.sink.helse.healthChecks
import no.nav.pia.bigquery.sink.konfigurasjon.Clusters
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaConfig
import no.nav.pia.bigquery.sink.konfigurasjon.NaisEnvironment
import no.nav.pia.bigquery.sink.mertics.metrics
import java.util.concurrent.TimeUnit

fun main() {
    val bigQueryClient: BigQueryClient = when (NaisEnvironment.cluster) {
        Clusters.LOKAL.clusterId -> LocalBigQueryClient()
        else -> DefaultBigQueryClient(NaisEnvironment.team_project_id)
    }
    val bigQueryService: BigQueryService = BigQueryService(
        bigQueryClient,
    ).apply {
        migrate(schemaRegistry)
    }

    val kafkaConfig = KafkaConfig()

    val bigQueryHendelseMottak = BigQueryHendelseMottak(bigQueryService)

    kafkaConfig.generelleTopics.forEach { topic ->
        PiaKafkaLytter(kafkaConfig = kafkaConfig, bigQueryHendelseMottak = bigQueryHendelseMottak, topic = topic).apply {
            run()
        }.also { HelseMonitor.leggTilHelsesjekk(it) }
    }

    SamarbeidsplanConsumer(kafkaConfig = kafkaConfig, bigQueryHendelseMottak = bigQueryHendelseMottak).apply {
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
