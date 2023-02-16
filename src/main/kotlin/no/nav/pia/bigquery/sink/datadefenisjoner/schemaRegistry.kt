package no.nav.pia.bigquery.sink.datadefenisjoner

import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import no.nav.pia.bigquery.sink.BigQuery
import no.nav.pia.bigquery.sink.Config
import no.nav.pia.bigquery.sink.Gcp
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.ia_sak_hendelser_v1
import no.nav.pia.bigquery.sink.schema.Registry

fun Map<String, Any?>.toRowToInsert(): RowToInsert = RowToInsert.of(this)

val schemaRegistry = Registry(
    DatasetId.of(Config[Gcp.team_project_id], Config[BigQuery.dataset_id]),
    ia_sak_hendelser_v1.entry(),
)
