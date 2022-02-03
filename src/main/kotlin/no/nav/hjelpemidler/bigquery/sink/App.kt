package no.nav.hjelpemidler.bigquery.sink

import com.google.cloud.bigquery.DatasetId
import no.nav.helse.rapids_rivers.RapidApplication

fun main() {
    val schemaRegistry = SchemaRegistry.create("/db/schema")
    val bigQueryClient: BigQueryClient = when (Config.environment) {
        Environment.LOCAL -> LocalBigQueryClient()
        else -> DefaultBigQueryClient(
            DatasetId.of(
                Config[Gcp.team_project_id],
                Config[BigQuery.dataset_id],
            ),
            schemaRegistry
        )
    }

    bigQueryClient.migrate()

    RapidApplication
        .create(Config.asMap())
        .also { rapidsConnection ->
            BigQueryHendelseMottak(rapidsConnection, bigQueryClient)
        }
        .start()
}
