package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition

val `samarbeidsplan-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "samarbeidsplan",
        version = 1,
    )

    override fun define(): TableDefinition =
        standardTableDefinition {
            schema {
                integer(name = "id") {
                    required()
                    description(description = "id til et undertema i en plan")
                }
                integer(name = "tema_id") {
                    required()
                    description(description = "id til temaet som dette undertemaet er en del av")
                }
                string(name = "plan_id") {
                    required()
                    description(description = "id til samarbeidsplanen som dette undertemaet er en del av")
                }
                integer(name = "samarbeid_id") {
                    required()
                    description(description = "id til samarbeidet planen er knyttet til")
                }
                string(name = "navn") {
                    required()
                    description(description = "navn på dette undertemaet")
                }
                string(name = "temanavn") {
                    required()
                    description(description = "navn på temaet dette undertemaet hører til")
                }
                boolean(name = "inkludert") {
                    required()
                    description(description = "om dette undertemaet er inkludert i planen eller ikke")
                }
                timestamp(name = "sist_endret_tidspunkt_plan") {
                    required()
                    description(
                        description = "Tidspunkt for når planen sist ble endret, det betyr ikke at dette undertemaet har blitt endret",
                    )
                }
                timestamp(name = "tidsstempel") {
                    required()
                    description(description = "Tidsstempel for lagring i BigQuery")
                }
                string(name = "status") {
                    description(description = "status på innhold i Plan")
                }
                date(name = "startDato") {
                    description(description = "Dato for når et innhold er planlagt å starte")
                }
                date(name = "sluttDato") {
                    description(description = "Dato for når et innhold er planlagt å avsluttes")
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
