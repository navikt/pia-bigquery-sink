package no.nav.hjelpemidler.bigquery.sink.registry

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.asObject
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val hendelse_v2 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "hendelse",
        version = 2,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
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
            string("geografisk_tilknytning") {
                nullable()
                description("F.eks. sentral eller kommune hendelsen er knyttet til")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av hendelsen")
            }
        }
        timePartitioning(TimePartitioning.Type.MONTH) {
            setField("opprettet")
        }
        clustering {
            setFields(listOf("opprettet"))
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = mapOf(
        payload.use("opprettet") { asDateTime() },
        payload.use("navn") { asText() },
        payload.use("kilde") { asText() },
        payload.use("data") {
            asObject<Map<String, String>>().map {
                mapOf("navn" to it.key, "verdi" to it.value)
            }
        },
        payload["geografiskTilknytning"].textValue() to "geografisk_tilknytning",
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
