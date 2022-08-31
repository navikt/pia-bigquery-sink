package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.module.kotlin.contains
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDate
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val utbetaling_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "utbetaling",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("orgNr") {
                required()
            }
            string("batchId") {
                required()
            }
            integer("totalbelop") {
                nullable()
            }
            integer("antallLinjer") {
                required()
            }
            datetime("opprettet") {
                required()
            }
            string("kilde") {
                required()
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
        payload.use("opprettet") { asDateTime() },
        payload.use("orgnr") { textValue() },
        payload.use("totalbelop") { intValue() },
        payload.use("antallLinjer") { intValue() },
        payload.use("kilde") { textValue() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()

    override fun skip(payload: JsonNode): Boolean = payload.contains("data") // gammel event fra hm-brille-api
}
