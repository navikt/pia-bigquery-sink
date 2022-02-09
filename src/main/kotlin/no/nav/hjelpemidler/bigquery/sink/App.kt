package no.nav.hjelpemidler.bigquery.sink

import com.google.cloud.bigquery.DatasetId
import no.nav.helse.rapids_rivers.RapidApplication

fun main() {
    val datasetId = DatasetId.of(Config[Gcp.team_project_id], Config[BigQuery.dataset_id])
    val schemaRegistry = SchemaRegistry.create()
    val bigQueryClient: BigQueryClient = when (Config.environment) {
        Environment.LOCAL -> LocalBigQueryClient()
        else -> DefaultBigQueryClient(datasetId)
    }
    val bigQueryService: BigQueryService = BigQueryService(datasetId, schemaRegistry, bigQueryClient).apply {
        migrate()
    }

    RapidApplication
        .create(Config.asMap())
        .also { rapidsConnection ->
            BigQueryHendelseMottak(rapidsConnection, bigQueryService)
        }
        .start()
}
