package no.nav.hjelpemidler.bigquery.sink.registry

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.asDateTime
import no.nav.hjelpemidler.bigquery.sink.asObject
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition
import no.nav.hjelpemidler.bigquery.sink.toText
import no.nav.hjelpemidler.bigquery.sink.use

val bestillingsordning_v2 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "bestillingsordning",
        version = 2,
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
            string("produkter") {
                repeated()
                description("Produkter (hmsnr) det søkes om")
            }
            string("produkter_ikke_pa_bestillingsordning") {
                repeated()
                description("Produkter (hmsnr) ikke på bestillingsordning")
            }
            boolean("bruker_har_hjelpemidler_fra_for") {
                required()
                description("Bruker har hjelpemidler fra før")
            }
            boolean("bruker_har_infotrygd_vedtak_fra_for") {
                required()
                description("Bruker har vedtak i Infotrygd fra før")
            }
            boolean("bruker_har_hotsak_vedtak_fra_for") {
                description("Bruker har vedtak i Hotsak fra før")
            }
            string("kommunenavn") {
                required()
                description("Kommunen som innsender tilhører")
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
        payload.use("produkter") { asObject<Set<String>>() },
        payload.use("produkter_ikke_pa_bestillingsordning") { asObject<Set<String>>() },
        "bruker_har_hjelpemidler_fra_for" to (payload["bruker_har_hjelpemidler_fra_for"]?.asBoolean() ?: false),
        "bruker_har_infotrygd_vedtak_fra_for" to (payload["bruker_har_infotrygd_vedtak_fra_for"]?.asBoolean() ?: false),
        "bruker_har_hotsak_vedtak_fra_for" to (payload["bruker_har_hotsak_vedtak_fra_for"]?.asBoolean() ?: false),
        payload["kommunenavn"] toText "kommunenavn",
        "tidsstempel" to "AUTO",
    ).toRowToInsert()
}
