package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition

val `behovsvurdering-bigquery-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "behovsvurdering-bigquery",
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
                integer("samarbeidId") {
                    required()
                    description("Id til samarbeidet behovsvurderingen er knyttet til")
                }
                string("opprettetAv") {
                    required()
                    description("Navident til rådgiver som opprettet behovsvurderingen")
                }
                timestamp("opprettet") {
                    required()
                    description("Tidspunkt for opprettelse av behovsvurdering")
                }
                boolean("harMinstEttSvar") {
                    required()
                    description("Om behovsvurderingen har fått minst ett svar")
                }
                timestamp("endret") {
                    description(
                        "Tidspunkt for sist endring av behovsvurdering",
                    )
                }
                timestamp("påbegynt") {
                    description(
                        "Tidspunkt for når behovsvurderingen ble påbegynt",
                    )
                }
                timestamp("fullført") {
                    description(
                        "Tidspunkt for når behovsvurderingen ble fullført",
                    )
                }
                timestamp("førsteSvarMotatt") {
                    description(
                        "Tidspunkt for når behovsvurderingen fikk første svar",
                    )
                }
                timestamp("sisteSvarMottatt") {
                    description(
                        "Tidspunkt for når behovsvurderingen fikk det siste svaret",
                    )
                }
                timestamp("tidsstempel") {
                    required()
                    description("Tidsstempel for lagring i BigQuery")
                }
            }
        }

    @Deprecated(
        "Ikke bruk denne transformasjonen",
        ReplaceWith("Kafka-consumer som bruker Serializable og ikke denne transformasjonen"),
    )
    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert =
        throw IllegalArgumentException("Transformasjon er Deprecated for ${schemaId.name}")
}
