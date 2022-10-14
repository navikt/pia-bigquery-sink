package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val slettede_vedtak_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "slettede_vedtak",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            integer("vedtak_id") {
                required()
            }
            datetime("opprettet") {
                required()
            }
            timestamp("tidsstempel") {
                required()
            }
            string("slettet_av_type") {
                nullable()
            }
        }
        timePartitioning(TimePartitioning.Type.MONTH) {
            setField("opprettet")
        }
        clustering {
            setFields(listOf("opprettet"))
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("vedtak_id") { longValue() },
        payload.use("opprettet") { asDateTime() },
        "tidsstempel" to "AUTO",
        payload.use("slettet_av_type") { textValue() },
    ).toRowToInsert()
}
