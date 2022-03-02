package no.nav.hjelpemidler.bigquery.sink.registry

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.asObject
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val tilbakeforing_gosys_tilbakemelding_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "tilbakeforing_gosys_tilbakemelding",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            datetime("opprettet") {
                required()
                description("Dato og klokkeslett for tilbakeføring")
            }
            string("saksnummer") {
                required()
                description("Saksnummer i HOTSAK")
            }
            string("enhetsnummer") {
                required()
                description("Hjelpemiddelsentralens enhetsnummer")
            }
            string("enhetsnavn") {
                nullable()
                description("Hjelpemiddelsentralens navn")
            }
            string("dokumentbeskrivelse") {
                nullable()
                description("Beskrivelse av dokumentet")
            }
            string("valgte_arsaker") {
                repeated()
                description("Valgte årsaker til tilbakeføring")
            }
            string("begrunnelse") {
                nullable()
                description("Valgfri begrunnelse for tilbakeføring")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av tilbakemeldingen")
            }
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = mapOf(
        payload.use("opprettet") { asDateTime() },
        payload.use("saksnummer") { asText() },
        payload.use("enhetsnummer") { asText() },
        payload.use("enhetsnavn") { asText() },
        payload.use("dokumentbeskrivelse") { asText() },
        payload.use("valgte_arsaker") { asObject<Set<String>>() },
        payload.use("begrunnelse") { asText() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
