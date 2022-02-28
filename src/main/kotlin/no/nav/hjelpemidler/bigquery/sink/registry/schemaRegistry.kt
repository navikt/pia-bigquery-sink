package no.nav.hjelpemidler.bigquery.sink.registry

import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition

val schemaRegistry: Map<SchemaDefinition.Id, SchemaDefinition> = mapOf(
    hendelse_v1.entry(),
    hendelse_v2.entry(),
    //hjelpemiddelstatistikk_v1.entry(),
    tilbakeforing_gosys_tilbakemelding_v1.entry(),
    saksstatistikk_v1.entry()
)
