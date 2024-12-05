package no.nav.pia.bigquery.sink.datadefenisjoner

import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-leveranse-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`ia-sak-statistikk-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeid-bigquery-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeidsplan-bigquery-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeidsplan-innhold-bigquery-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`samarbeidsplan-tema-bigquery-v1`
import no.nav.pia.bigquery.sink.datadefenisjoner.fia.`sporreundersokelse-v1`
import no.nav.pia.bigquery.sink.konfigurasjon.BigQuery
import no.nav.pia.bigquery.sink.konfigurasjon.NaisEnvironment
import no.nav.pia.bigquery.sink.schema.Registry

fun Map<String, Any?>.toRowToInsert(): RowToInsert = RowToInsert.of(this)

val DATASET_ID: DatasetId = DatasetId.of(NaisEnvironment.team_project_id, BigQuery.dataset_id)

val schemaRegistry = Registry(
    DATASET_ID,
    `ia-sak-statistikk-v1`.entry(),
    `ia-sak-leveranse-v1`.entry(),
    `sporreundersokelse-v1`.entry(),
    `samarbeid-bigquery-v1`.entry(),
    `samarbeidsplan-bigquery-v1`.entry(),
    `samarbeidsplan-tema-bigquery-v1`.entry(),
    `samarbeidsplan-innhold-bigquery-v1`.entry(),
)
