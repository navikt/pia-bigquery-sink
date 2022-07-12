package no.nav.hjelpemidler.bigquery.sink

import no.nav.helse.rapids_rivers.RapidApplication
import no.nav.hjelpemidler.bigquery.sink.brille.brilleRegistry
import no.nav.hjelpemidler.bigquery.sink.registry.schemaRegistry

fun main() {
    val bigQueryClient: BigQueryClient = when (Config.environment) {
        Environment.LOCAL -> LocalBigQueryClient()
        else -> DefaultBigQueryClient(Config[Gcp.team_project_id])
    }
    val bigQueryService: BigQueryService = BigQueryService(Config[Gcp.team_project_id], bigQueryClient).apply {
        migrate(brilleRegistry)
        migrate(schemaRegistry)
    }

    RapidApplication
        .create(Config.asMap())
        .also { rapidsConnection ->
            BigQueryHendelseMottak(rapidsConnection, bigQueryService)
        }
        .start()
}
