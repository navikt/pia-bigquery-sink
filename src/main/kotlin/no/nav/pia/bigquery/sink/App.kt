package no.nav.pia.bigquery.sink

import no.nav.pia.bigquery.sink.datadefenisjoner.schemaRegistry
import no.nav.pia.bigquery.sink.konfigurasjon.KafkaKonfigurasjon

fun main() {
    val bigQueryClient: BigQueryClient = when (Config.environment) {
        no.nav.pia.bigquery.sink.Environment.LOCAL -> LocalBigQueryClient()
        else -> DefaultBigQueryClient(Config[Gcp.team_project_id])
    }
    val bigQueryService: BigQueryService = BigQueryService(
        Config[Gcp.team_project_id],
        bigQueryClient
    ).apply {
        migrate(schemaRegistry)
    }

    PiaKafkaLytter.apply {
        create(
            kafkaKonfigurasjon = KafkaKonfigurasjon(),
            bigQueryHendelseMottak = BigQueryHendelseMottak(bigQueryService)
        )
        run()
    }
}
