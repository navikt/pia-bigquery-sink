package no.nav.hjelpemidler.bigquery.sink.registry

import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import no.nav.hjelpemidler.bigquery.sink.BigQuery
import no.nav.hjelpemidler.bigquery.sink.Config
import no.nav.hjelpemidler.bigquery.sink.Gcp
import no.nav.hjelpemidler.bigquery.sink.schema.Registry

fun Map<String, Any?>.toRowToInsert(): RowToInsert = RowToInsert.of(this)

val schemaRegistry = Registry(
    DatasetId.of(Config[Gcp.team_project_id], Config[BigQuery.dataset_id]), mapOf(
        hendelse_v1.entry(),
        hendelse_v2.entry(),
        hjelpemiddelstatistikk_v1.entry(),
        tilbakeforing_gosys_tilbakemelding_v1.entry(),
        saksstatistikk_v1.entry(),
        bestillingsordning_v1.entry(),
        bestillingsordning_v2.entry(),
        bestillingsordning_v3.entry(),
    )
)
