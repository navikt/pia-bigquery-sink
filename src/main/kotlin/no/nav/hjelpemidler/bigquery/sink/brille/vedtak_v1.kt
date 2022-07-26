package no.nav.hjelpemidler.bigquery.sink.brille

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDate
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.intValueWithName
import no.nav.hjelpemidler.bigquery.sink.registry.toRowToInsert
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.textValueWithName
import no.nav.hjelpemidler.bigquery.sink.use

val vedtak_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "vedtak",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            string("orgnr") {
                required()
            }
            string("org_navn") {
                required()
            }
            integer("barnets_alder") {
                nullable()
            }
            decimal("hoyre_sfere") {
                required()
            }
            decimal("hoyre_sylinder") {
                required()
            }
            decimal("venstre_sfere") {
                required()
            }
            decimal("venstre_sylinder") {
                required()
            }
            date("bestillingsdato") {
                required()
            }
            decimal("brillepris") {
                required()
            }
            string("behandlingsresultat") {
                required()
            }
            string("sats") {
                required()
            }
            integer("sats_belop") {
                required()
            }
            string("sats_beskrivelse") {
                required()
            }
            decimal("belop") {
                required()
            }
            datetime("opprettet") {
                required()
            }
            timestamp("tidsstempel") {
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
        payload["orgNavn"] textValueWithName "org_navn",
        payload["barnetsAlder"] intValueWithName "barnets_alder",
        "hoyre_sfere" to payload.at("/brilleseddel/høyreSfære").doubleValue(),
        "hoyre_sylinder" to payload.at("/brilleseddel/høyreSylinder").doubleValue(),
        "venstre_sfere" to payload.at("/brilleseddel/venstreSfære").doubleValue(),
        "venstre_sylinder" to payload.at("/brilleseddel/venstreSylinder").doubleValue(),
        payload.use("bestillingsdato") { asDate() },
        payload.use("brillepris") { textValue() },
        payload.use("behandlingsresultat") { textValue() },
        payload.use("sats") { textValue() },
        payload["satsBeløp"] intValueWithName "sats_belop",
        payload["satsBeskrivelse"] textValueWithName "sats_beskrivelse",
        "belop" to payload["beløp"].decimalValue().toString(),
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
