package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition

val `samarbeidsplan-bigquery-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "samarbeidsplan-bigquery",
        version = 1,
    )

    override fun define(): TableDefinition =
        standardTableDefinition {
            schema {
                string("id") {
                    required()
                    description("Id til samarbeidsplan")
                }
                integer("samarbeidId") {
                    required()
                    description("Id til samarbeidet planen er knyttet til")
                }
                timestamp("endret") {
                    required()
                    description(
                        "Tidspunkt for sist endring av behovsvurdering, settes til opprettetTidspunkt ved sending til Bigquery om ikke eksiterende",
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

val `samarbeidsplan-tema-bigquery-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "samarbeidsplan-tema-bigquery",
        version = 1,
    )

    override fun define(): TableDefinition =
        standardTableDefinition {
            schema {
                integer("id") {
                    required()
                    description("Id til et tema i en plan")
                }
                string("navn") {
                    required()
                    description("navn på temaet")
                }
                boolean("inkludert") {
                    required()
                    description("Om et tema er inkludert i en plan eller ikke")
                }
                string("planId") {
                    required()
                    description("Id til samarbeidsplan")
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

val `samarbeidsplan-innhold-bigquery-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "samarbeidsplan-innhold-bigquery",
        version = 1,
    )

    override fun define(): TableDefinition =
        standardTableDefinition {
            schema {
                integer("id") {
                    required()
                    description("Id til et tema i en plan")
                }
                string("navn") {
                    required()
                    description("navn på innholdet")
                }
                boolean("inkludert") {
                    required()
                    description("Om et tema er inkludert i en plan eller ikke")
                }
                integer("temaId") {
                    required()
                    description("Id til et tema i en plan")
                }
                string("status") {
                    description("status på innhold i Plan")
                }
                date("startDato") {
                    description("Dato for når et innhold er planlagt å starte")
                }
                date("sluttDato") {
                    description("Dato for når et innhold er planlagt å avsluttes")
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
