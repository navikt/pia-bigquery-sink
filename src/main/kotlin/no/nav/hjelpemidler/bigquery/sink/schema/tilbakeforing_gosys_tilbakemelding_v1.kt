package no.nav.hjelpemidler.bigquery.sink.schema

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.Schema
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.asObject
import no.nav.hjelpemidler.bigquery.sink.use

val tilbakeforing_gosys_tilbakemelding_v1 = object : SchemaDefinition {
    override val schemaId: SchemaId = SchemaId(name = "tilbakeforing_gosys_tilbakemelding", version = 1)

    override fun define(): Schema = schema {
        string("saksnummer") {
            required()
            description("Saksnummer i HOTSAK")
        }
        datetime("opprettet") {
            required()
            description("Dato og klokkeslett for tilbakeføring")
        }
        string("enhetsnummer") {
            required()
            description("Hjelpemiddelsentralens enhetsnummer")
        }
        string("enhetsnavn") {
            nullable()
            description("Hjelpemiddelsentralens navn")
        }
        string("dokument_beskrivelse") {
            nullable()
            description("Beskrivelse av dokumentet")
        }
        string("tilbakemelding_valgte_arsaker") {
            repeated()
            description("Valgte årsaker til tilbakeføring")
        }
        string("tilbakemelding_begrunnelse") {
            nullable()
            description("Valgfri begrunnelse for tilbakeføring")
        }
        timestamp("tidsstempel") {
            required()
            description("Tidsstempel for lagring av tilbakemeldingen")
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = RowToInsert.of(mapOf(
        payload.use("saksnummer") { asText() },
        payload.use("opprettet") { asDateTime() },
        payload.use("enhetsnummer") { asText() },
        payload.use("enhetsnavn") { asText() },
        payload.use("dokument_beskrivelse") { asText() },
        payload.use("tilbakemelding_valgte_arsaker") { asObject<Set<String>>() },
        payload.use("tilbakemelding_begrunnelse") { asText() },
        "tidsstempel" to "AUTO",
    ))
}
