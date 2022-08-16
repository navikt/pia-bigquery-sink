package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.use

val medlemskap_folketrygden_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "medlemskap_folketrygden",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            integer("vedtakId") {
                required()
                description("Vedtak id er alltid -1 når statistikken sendes ved oppslag, men er satt til vedtaks id når vedtaket blir opprettet")
            }
            boolean("bevist") {
                required()
                description("Medlemskapet er bevist")
            }
            boolean("antatt") {
                required()
                description("Medlemskapet er antatt")
            }
            boolean("avvist") {
                required()
                description("Medlemskapet kunne ikke bevises eller antas og ble derfor avvist")
            }
            datetime("opprettet") {
                required()
                description("Tidsstempel for opprettelse av avtalen")
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av avtalen")
            }
        }
    }

    override fun transform(payload: JsonNode): InsertAllRequest.RowToInsert = mapOf(
        payload.use("vedtakId") { longValue() },
        payload.use("bevist") { booleanValue() },
        payload.use("antatt") { booleanValue() },
        payload.use("avvist") { booleanValue() },
        payload.use("opprettet") { asDateTime() },
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
