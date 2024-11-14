package no.nav.pia.bigquery.sink.datadefenisjoner

import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`behovsvurdering-bigquery-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-leveranse-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-statistikk-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeid-bigquery-v1`
import no.nav.pia.bigquery.sink.konfigurasjon.BigQuery
import no.nav.pia.bigquery.sink.konfigurasjon.NaisEnvironment
import no.nav.pia.bigquery.sink.schema.Registry

fun Map<String, Any?>.toRowToInsert(): RowToInsert = RowToInsert.of(this)

val schemaRegistry = Registry(
    DatasetId.of(NaisEnvironment.team_project_id, BigQuery.dataset_id),
    `ia-sak-statistikk-v1`.entry(),
    `ia-sak-leveranse-v1`.entry(),
    `behovsvurdering-bigquery-v1`.entry(),
    `samarbeid-bigquery-v1`.entry(),
)
