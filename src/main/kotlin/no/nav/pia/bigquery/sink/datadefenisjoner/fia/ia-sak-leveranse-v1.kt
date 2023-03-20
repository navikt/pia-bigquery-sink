package no.nav.pia.bigquery.sink.datadefenisjoner.fia

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.pia.bigquery.sink.asLocalDate
import no.nav.pia.bigquery.sink.asUtcDateTime
import no.nav.pia.bigquery.sink.datadefenisjoner.toRowToInsert
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import no.nav.pia.bigquery.sink.schema.standardTableDefinition
import no.nav.pia.bigquery.sink.use

val `ia-sak-leveranse-v1` = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "ia-sak-leveranse",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            integer("id") {
                required()
                description("Autogenerert id for ia-leveransen")
            }
            string("saksnummer") {
                required()
                description("Saksnummer")
            }
            integer("iaModulId") {
                required()
                description("Id til levert ia-modul")
            }
            string("iaModulNavn") {
                required()
                description("Navn på levert modul innenfor en ia-tjeneste")
            }
            integer("iaTjenesteId") {
                required()
                description("Id til levert ia-tjeneste")
            }
            string("iaTjenesteNavn") {
                required()
                description("Navn på levert ia-tjeneste som modulen tilhører")
            }
            date("frist") {
                required()
                description("Tentativ frist satt for leveransen")
            }
            string("status") {
                required()
                description("Leveranse status")
            }
            string("opprettetAv") {
                required()
                description("Navidenten til rådgiver som opprettet leveransen")
            }
            timestamp("sistEndret") {
                required()
                description("Tidspunkt for siste oppdatering av leveranse")
            }
            string("sistEndretAv") {
                required()
                description("Navidenten til rådgiver som sist oppdaterte leveransen")
            }
            string("sistEndretAvRolle") {
                description("Rollen til rådgiver som sist oppdaterte leveransen")
            }
            timestamp("fullfort") {
                description("Tidspunkt for fullføring av leveranse")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring i BigQuery")
            }
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("id") { asInt() },
        payload.use("saksnummer") { textValue() },

        payload.use(jsonPropery = "modul", databaseColumn = "iaModulId") { get("id").asInt() },
        payload.use(jsonPropery = "modul", databaseColumn = "iaModulNavn") { get("navn").textValue() },
        payload.use(jsonPropery = "modul", databaseColumn = "iaTjenesteId") { get("iaTjeneste").get("id").asInt() },
        payload.use(jsonPropery = "modul", databaseColumn = "iaTjenesteNavn") { get("iaTjeneste").get("navn").textValue() },

        payload.use("frist") { asLocalDate() },
        payload.use("status") { textValue() },
        payload.use("opprettetAv") { textValue() },
        payload.use("sistEndret") { asUtcDateTime() },
        payload.use("sistEndretAv") { textValue() },
        payload.use("sistEndretAvRolle") { textValue() },
        payload.use(jsonPropery = "fullført", databaseColumn = "fullfort") { asUtcDateTime() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}

private fun <T> JsonNode.use(jsonPropery: String, databaseColumn: String, transform: JsonNode.() -> T): Pair<String, T?> = databaseColumn to get(jsonPropery)?.let {
    transform(it)
}