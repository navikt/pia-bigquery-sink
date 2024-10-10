package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.asUtcDateTime
import no.nav.pia.bigquery.sink.datadefenisjoner.toRowToInsert
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition
import no.nav.pia.bigquery.sink.use

val `behovsvurdering-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "behovsvurdering",
        version = 1,
    )

    override fun define(): TableDefinition =
        standardTableDefinition {
            schema {
                string("id") {
                    required()
                    description("Id til behovsvurderingen")
                }
                string("orgnr") {
                    required()
                    description("Virksomhetsnummer (b.nummer)")
                }
                string("status") {
                    required()
                    description("Status som viser om behovsvurdering er opprettet, påbegynt eller avsluttet")
                }
                string("opprettetAv") {
                    required()
                    description("Navident til rådgiver som opprettet behovsvurderingen")
                }
                timestamp("opprettet") {
                    required()
                    description("Tidspunkt for opprettelse av behovsvurdering")
                }
                timestamp("endret") {
                    required()
                    description("Tidspunkt for sist endring av behovsvurdering")
                }
                integer("samarbeidId") {
                    required()
                    description("Id til samarbeidet behovsvurderingen er knyttet til")
                }
                timestamp("tidsstempel") {
                    required()
                    description("Tidsstempel for lagring i BigQuery")
                }
            }
        }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert =
        mapOf(
            payload.use("id") { textValue() },
            payload.use("orgnr") { textValue() },
            payload.use("status") { textValue() },
            payload.use("opprettetAv") { textValue() },
            payload.use("opprettet") { asUtcDateTime() },
            payload.use("endret") { asUtcDateTime() },
            payload.use("samarbeidId") { intValue() },
            "tidsstempel" to "AUTO",
        ).toRowToInsert()
}
