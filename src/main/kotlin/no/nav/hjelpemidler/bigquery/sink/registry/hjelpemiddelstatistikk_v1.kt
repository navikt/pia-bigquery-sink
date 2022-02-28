package no.nav.hjelpemidler.bigquery.sink.registry

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val hjelpemiddelstatistikk_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "hjelpemiddelstatistikk",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("hmsnr") {
                nullable()
                description("")
            }
            string("isokode") {
                nullable()
                description("")
            }
            string("isotittel") {
                nullable()
                description("")
            }
            string("produktnavn") {
                nullable()
                description("")
            }
            string("artikkelnavn") {
                nullable()
                description("")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av hendelsen")
            }
        }
        timePartitioning(TimePartitioning.Type.MONTH) {
            setField("tidsstempel")
        }
        clustering {
            setFields(listOf("tidsstempel"))
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = RowToInsert.of(mapOf(
        payload.use("hmsnr") { asText() },
        "tidsstempel" to "AUTO",
    ))
}
