package no.nav.hjelpemidler.bigquery.sink.registry

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.toText
import no.nav.hjelpemidler.bigquery.sink.use

// Opprettet ifbm. at sjekk mot bestillingsordning nå blir utført i hm-soknad-api
val bestillingsordning_v3 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "bestillingsordning",
        version = 3,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            datetime("opprettet") {
                required()
                description("Dato og klokkeslett for hendelsen")
            }
            string("soknadid") {
                description("Søknad id")
            }
            string("version") {
                description("Versjon av hm-bestillingsordning-sjekker det ble sjekket mot")
            }
            boolean("kan_vere_bestilling") {
                required()
                description("Om søknaden kan være bestilling")
            }
            boolean("alle_hoved_produkter_pa_bestillingsordning") {
                required()
                description("Alle hovedproduktene er på bestillingsordning")
            }
            boolean("alle_tilbehor_pa_bestillingsordning") {
                required()
                description("Alle tilbehørene er på bestillingsordning")
            }
            boolean("bruker_har_hjelpemidler_fra_for") {
                description("Bruker har hjelpemidler fra før")
            }
            boolean("bruker_har_infotrygd_vedtak_fra_for") {
                description("Bruker har vedtak i Infotrygd fra før")
            }
            boolean("bruker_har_hotsak_vedtak_fra_for") {
                description("Bruker har vedtak i Hotsak fra før")
            }
            boolean("levering_til_folkeregistrert_adresse") {
                description("Utlevering er satt til brukers folkeregistrerte adresse")
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
        payload["soknadid"] toText "soknadid",
        payload["version"] toText "version",
        "kan_vere_bestilling" to (payload["kan_vere_bestilling"]?.asBoolean() ?: false),
        "alle_hoved_produkter_pa_bestillingsordning" to (
            payload["alle_hoved_produkter_pa_bestillingsordning"]?.asBoolean()
                ?: false
            ),
        "alle_tilbehor_pa_bestillingsordning" to (payload["alle_tilbehor_pa_bestillingsordning"]?.asBoolean() ?: false),
        "bruker_har_hjelpemidler_fra_for" to (payload["bruker_har_hjelpemidler_fra_for"]?.asBoolean() ?: false),
        "bruker_har_infotrygd_vedtak_fra_for" to (payload["bruker_har_infotrygd_vedtak_fra_for"]?.asBoolean() ?: false),
        "bruker_har_hotsak_vedtak_fra_for" to (payload["bruker_har_hotsak_vedtak_fra_for"]?.asBoolean() ?: false),
        "levering_til_folkeregistrert_adresse" to (payload["levering_til_folkeregistrert_adresse"]?.asBoolean() ?: false),
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
