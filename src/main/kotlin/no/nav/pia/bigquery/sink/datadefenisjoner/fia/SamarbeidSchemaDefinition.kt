package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition

val `samarbeid-bigquery-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "samarbeid",
        version = 1,
    )

    override fun define(): TableDefinition =
        standardTableDefinition {
            schema {
                // Required
                integer("id") {
                    required()
                    description("Id til et samarbeid")
                }
                string("saksnummer") {
                    required()
                    description("Saksnummer samarbeidet er knyttet til")
                }
                timestamp("opprettet") {
                    required()
                    description("Tidspunkt for når samarbeidet ble opprettet")
                }
                timestamp("tidsstempel") {
                    required()
                    description("Tidspunkt for lagring i BigQuery")
                }
                // Optional
                string("navn") {
                    description("Navn på samarbeidet")
                }
                string("status") {
                    description("Status på samarbeidet")
                }
                timestamp("avbrutt") {
                    description("Tidspunkt for når samarbeidet ble avbrutt")
                }
                timestamp("fullfort") {
                    description("Tidspunkt for når samarbeidet ble fullført")
                }
                timestamp("endret") {
                    description("Tidspunkt for når samarbeidet sist ble endret")
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
