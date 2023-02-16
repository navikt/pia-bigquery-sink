package no.nav.pia.bigquery.sink.datadefenisjoner

import com.google.cloud.bigquery.DatasetId
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class SchemaRegistryTest {
    @Test
    internal fun `creating arbitrary TableInfo should not fail`() {
        val definition = schemaRegistry.values.first()
        val datasetId = DatasetId.of("project", "dataset")
        val tableInfo = definition.toTableInfo(datasetId)
        tableInfo.tableId shouldBe definition.schemaId.toTableId(datasetId)
    }

//    @Test
//    internal fun `payload transformed to bestillingsordning_v4 row`() {
//        val payload = jsonMapper.readTree(
//            """{
//             "opprettet": "${LocalDateTime.now()}",
//             "soknadid": "abc-123-def-456",
//             "version": "v1.2.3",
//             "kan_vere_bestilling": "true",
//             "alle_hoved_produkter_pa_bestillingsordning": "true",
//             "alle_tilbehor_pa_bestillingsordning": "true",
//             "bruker_har_hjelpemidler_fra_for":  "null",
//             "bruker_har_infotrygd_vedtak_fra_for":"false",
//             "bruker_har_hotsak_vedtak_fra_for": "true",
//             "levering_til_folkeregistrert_adresse": "true",
//             "bruker_bor_ikke_pa_institusjon": "true",
//             "bruker_bor_ikke_i_utlandet": "true",
//             "bruker_er_ikke_skjermet_person": "true",
//             "inneholder_ikke_fritekst": "true",
//             "produkter": ["123"],
//             "tilbehor": ["456"],
//             "produkter_ikke_pa_bestillingsordning": [],
//             "tilbehor_ikke_pa_bestillingsordning": [],
//             "kategorier": ["terskeleliminator"]
//            }""".trimMargin()
//        )
//
//        val content = bestillingsordning_v4.transform(payload).content
//        content.shouldContain("tidsstempel" to "AUTO")
//        content.shouldContain("soknadid" to "abc-123-def-456")
//        content.shouldContain("version" to "v1.2.3")
//        content.shouldContain("alle_hoved_produkter_pa_bestillingsordning" to true)
//        content.shouldContain("alle_tilbehor_pa_bestillingsordning" to true)
//        content.shouldContain("bruker_har_hjelpemidler_fra_for" to false)
//        content.shouldContain("bruker_har_infotrygd_vedtak_fra_for" to false)
//        content.shouldContain("bruker_har_hotsak_vedtak_fra_for" to true)
//        content.shouldContain("levering_til_folkeregistrert_adresse" to true)
//        content.shouldContain("bruker_bor_ikke_pa_institusjon" to true)
//        content.shouldContain("bruker_bor_ikke_i_utlandet" to true)
//        content.shouldContain("bruker_er_ikke_skjermet_person" to true)
//        content.shouldContain("inneholder_ikke_fritekst" to true)
//        content.shouldContain("produkter" to setOf("123"))
//        content.shouldContain("tilbehor" to setOf("456"))
//        content.shouldContain("produkter_ikke_pa_bestillingsordning" to emptySet<String>())
//        content.shouldContain("tilbehor_ikke_pa_bestillingsordning" to emptySet<String>())
//        content.shouldContain("kategorier" to setOf("terskeleliminator"))
//    }
//
//    @Test
//    internal fun `payload transformed to hendelse_v1 row`() {
//        val payload = jsonMapper.readTree(
//            """{
//            "opprettet": "${LocalDateTime.now()}",
//            "navn": "navn",
//            "kilde": "kilde",
//            "data": {
//                "someKey": "someValue"
//            }
//        }
//            """.trimIndent()
//        )
//        val content = hendelse_v1.transform(payload).content
//        content shouldContain ("tidsstempel" to "AUTO")
//    }
//
//    @Test
//    internal fun `payload transformed to hendelse_v2 row`() {
//        val payload = jsonMapper.readTree(
//            """{
//            "opprettet": "${LocalDateTime.now()}",
//            "navn": "navn",
//            "kilde": "kilde",
//            "data": {
//                "someKey": "someValue"
//            }
//        }
//            """.trimIndent()
//        )
//        val content = hendelse_v2.transform(payload).content
//        content shouldContain ("tidsstempel" to "AUTO")
//    }
//
//    @Test
//    internal fun `payload transformed to saksstatistikk_v1 row`() {
//        val payload = jsonMapper.readTree(
//            """{
//            "sakId": "1",
//            "behandlingId": "1",
//            "saksbehandler": null,
//            "saksbehandlerIdent": null
//        }
//            """.trimIndent()
//        )
//        val content = saksstatistikk_v1.transform(payload).content
//        content shouldContain ("sak_id" to "1")
//        content shouldContain ("behandling_id" to "1")
//        content shouldContain ("tidsstempel" to "AUTO")
//    }
}
