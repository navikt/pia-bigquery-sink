package no.nav.hjelpemidler.bigquery.sink.brille

import com.google.cloud.bigquery.DatasetId
import no.nav.hjelpemidler.bigquery.sink.BigQuery
import no.nav.hjelpemidler.bigquery.sink.Config
import no.nav.hjelpemidler.bigquery.sink.Gcp
import no.nav.hjelpemidler.bigquery.sink.schema.Registry

val brilleRegistry = Registry(
    DatasetId.of(Config[Gcp.team_project_id], Config[BigQuery.brille_dataset_id]), mapOf(
        avtale_v1.entry(),
    )
)
