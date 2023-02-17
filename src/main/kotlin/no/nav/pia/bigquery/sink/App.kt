package no.nav.pia.bigquery.sink

import no.nav.pia.bigquery.sink.datadefenisjoner.schemaRegistry
import no.nav.pia.bigquery.sink.konfigurasjon.Clusters
import no.nav.pia.bigquery.sink.konfigurasjon.Kafka
import no.nav.pia.bigquery.sink.konfigurasjon.Miljø

fun main() {
    val bigQueryClient: BigQueryClient = when (Miljø.cluster) {
        Clusters.LOKAL.clusterId -> LocalBigQueryClient()
        else -> DefaultBigQueryClient(Miljø.team_project_id)
    }
    val bigQueryService: BigQueryService = BigQueryService(
        Miljø.team_project_id,
        bigQueryClient
    ).apply {
        migrate(schemaRegistry)
    }

    PiaKafkaLytter.apply {
        create(
            kafkaKonfigurasjon = Kafka(),
            bigQueryHendelseMottak = BigQueryHendelseMottak(bigQueryService)
        )
        run()
    }
}
