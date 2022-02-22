package no.nav.hjelpemidler.bigquery.sink.schema

val schemaRegistry: Map<SchemaId, SchemaDefinition> = mapOf(
    hendelse_v1.entry(),
    tilbakeforing_gosys_tilbakemelding_v1.entry(),
    saksstatistikk_v1.entry()
)
