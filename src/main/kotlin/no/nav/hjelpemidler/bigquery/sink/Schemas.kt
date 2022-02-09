package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.Schema

object HendelseSchema : SchemaDefinition {
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
        struct("stikkord", {
            repeated()
            description("Stikkord for hendelsen")
        }) {
            string("navn") {
                required()
            }
            string("verdi") {
                nullable()
            }
        }
        struct("data", {
            repeated()
            description("Tilleggsinformasjon til hendelsen")
        }) {
            string("navn") {
                required()
            }
            string("verdi") {
                nullable()
            }
        }
        timestamp("tidsstempel") {
            required()
            description("Tidsstempel for lagring av hendelsen")
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = RowToInsert.of(mapOf(
        "opprettet" to payload["opprettet"].asLocalDateTime().toString(),
        "navn" to payload["navn"].asText(),
        "kilde" to payload["kilde"].asText(),
        "stikkord" to payload["stikkord"].asKeyValueMap().toStructEntries(),
        "data" to payload["data"].asKeyValueMap().toStructEntries(),
        "tidsstempel" to "AUTO",
    ))
}
