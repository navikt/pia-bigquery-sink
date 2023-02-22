package no.nav.pia.bigquery.sink.datadefenisjoner

import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-v1`
import no.nav.pia.bigquery.sink.konfigurasjon.BigQuery
import no.nav.pia.bigquery.sink.konfigurasjon.Miljø
import no.nav.pia.bigquery.sink.schema.Registry

fun Map<String, Any?>.toRowToInsert(): RowToInsert = RowToInsert.of(this)

val schemaRegistry = Registry(
    DatasetId.of(Miljø.team_project_id, BigQuery.dataset_id),
    `ia-sak-v1`.entry(),
)
