package no.nav.hjelpemidler.bigquery.sink.brille

import com.google.cloud.bigquery.DatasetId
import no.nav.hjelpemidler.bigquery.sink.BigQuery
import no.nav.hjelpemidler.bigquery.sink.Config
import no.nav.hjelpemidler.bigquery.sink.Gcp
import no.nav.hjelpemidler.bigquery.sink.schema.Registry

val brilleRegistry = Registry(
    DatasetId.of(Config[Gcp.team_project_id], Config[BigQuery.brille_dataset_id]),
    avtale_v1.entry(),
    vedtak_v1.entry(),
    avslag_v1.entry(),
    medlemskap_folketrygden_v1.entry(),
    // vilkarsvurdering_v1.entry(),
)
