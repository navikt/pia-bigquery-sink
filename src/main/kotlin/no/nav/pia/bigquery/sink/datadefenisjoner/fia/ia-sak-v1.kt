package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.asDateTime
import no.nav.pia.bigquery.sink.datadefenisjoner.toRowToInsert
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition
import no.nav.pia.bigquery.sink.use

val `ia-sak-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "ia-sak",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("saksnummer") {
                required()
                description("Id for saken")
            }
            string("orgnr") {
                required()
                description("Orgnr. for virksomheten")
            }
            string("eierAvSak") {
                description("Navidenten til rådgiver som eier saken")
            }
            string("endretAvHendelseId") {
                required()
                description("ID til hendelsen som som endret saken sist")
            }
            string("status") {
                required()
                description("Status på sak")
            }
            timestamp("opprettetTidspunkt") {
                required()
                description("Tidspunkt for opprettelse av sak")
            }
            timestamp("endretTidspunkt") {
                required()
                description("Tidspunkt for siste oppdatering av sak")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring i BigQuery")
            }
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("saksnummer") { textValue() },
        payload.use("orgnr") { textValue() },
        payload.use("eierAvSak") { textValue() },
        payload.use("endretAvHendelseId") { textValue() },
        payload.use("status") { textValue() },
        payload.use("opprettetTidspunkt") { asDateTime() },
        payload.use("endretTidspunkt") { asDateTime() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
