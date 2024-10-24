package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.datadefenisjoner.toRowToInsert
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition
import no.nav.pia.bigquery.sink.use

val `samarbeid-bigquery-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "samarbeid-bigquery",
        version = 1,
    )

    override fun define(): TableDefinition =
        standardTableDefinition {
            schema {
                integer("id") {
                    required()
                    description("Id til samarbeidet behovsvurderingen er knyttet til")
                }
                string("saksnummer") {
                    required()
                    description("Saksnummer")
                }
                string("navn") {
                    description("Navn på samarbeid")
                }
                string("status") {
                    description("Status om et samarbeid er aktivt eller har blitt slettet")
                }
                timestamp("tidsstempel") {
                    required()
                    description("Tidsstempel for lagring i BigQuery")
                }
            }
        }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert =
        mapOf(
            payload.use("id") { intValue() },
            payload.use("saksnummer") { textValue() },
            payload.use("navn") { textValue() },
            payload.use("status") { textValue() },
            "tidsstempel" to "AUTO",
        ).toRowToInsert()
}
