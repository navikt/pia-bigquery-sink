package no.nav.hjelpemidler.bigquery.sink.schema

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.Schema
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.asObject
import no.nav.hjelpemidler.bigquery.sink.use

val hendelse_v1 = object : SchemaDefinition {
    override val schemaId: SchemaId = SchemaId(name = "hendelse", version = 1)

    override fun define(): Schema = schema {
        datetime("opprettet") {
            required()
            description("Dato og klokkeslett for hendelsen")
        }
        string("navn") {
            required()
            description("Navn på hendelsen")
        }
        string("kilde") {
            required()
            description("Systemet hendelsen oppsto i")
        }
        struct("data") {
            repeated()
            description("Tilleggsinformasjon til hendelsen")
            subFields {
                string("navn") {
                    required()
                }
                string("verdi") {
                    nullable()
                }
            }
        }
        timestamp("tidsstempel") {
            required()
            description("Tidsstempel for lagring av hendelsen")
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = RowToInsert.of(mapOf(
        payload.use("opprettet") { asDateTime() },
        payload.use("navn") { asText() },
        payload.use("kilde") { asText() },
        payload.use("data") {
            asObject<Map<String, String>>().map {
                mapOf("navn" to it.key, "verdi" to it.value)
            }
        },
        "tidsstempel" to "AUTO",
    ))
}
