package no.nav.hjelpemidler.bigquery.sink.registry

import com.google.cloud.bigquery.DatasetId
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
import no.nav.hjelpemidler.bigquery.sink.Hash
import no.nav.hjelpemidler.bigquery.sink.jsonMapper
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

internal class SchemaRegistryTest {
    @Test
    internal fun `creating arbitrary TableInfo should not fail`() {
        val definition = schemaRegistry.values.first()
        val datasetId = DatasetId.of("project", "dataset")
        val tableInfo = definition.toTableInfo(datasetId)
        tableInfo.tableId shouldBe definition.schemaId.toTableId(datasetId)
    }

    @Test
    internal fun `payload transformed to bestillingsordning_v1 row`() {
        val payload = jsonMapper.readTree(
            """{
             "opprettet": "${LocalDateTime.now()}",
             "produkter": [
                "123"
             ],
             "produkter_ikke_pa_bestillingsordning": [
                "123"
             ],
             "bruker_har_hjelpemidler_fra_for":  "true",
             "bruker_har_vedtak_fra_for":"true",
             "formidler_har_bestillerkurs": "true",
             "kommunenavn": "Oslo"
            }""".trimMargin()
        )
        val content = bestillingsordning_v1.transform(payload).content
        content.shouldContain("tidsstempel" to "AUTO")
        content.shouldContain("produkter" to setOf("123"))
        content.shouldContain("produkter_ikke_pa_bestillingsordning" to setOf("123"))
        content.shouldContain("bruker_har_hjelpemidler_fra_for" to true)
        content.shouldContain("bruker_har_vedtak_fra_for" to true)
        content.shouldContain("formidler_har_bestillerkurs" to true)
    }

    @Test
    internal fun `payload transformed to bestillingsordning_v2 row`() {
        val payload = jsonMapper.readTree(
            """{
             "opprettet": "${LocalDateTime.now()}",
             "produkter": [
                "123"
             ],
             "produkter_ikke_pa_bestillingsordning": [
                "123"
             ],
             "bruker_har_hjelpemidler_fra_for":  "true",
             "bruker_har_infotrygd_vedtak_fra_for":"true",
             "bruker_har_hotsak_vedtak_fra_for": "true",
             "kommunenavn": "Oslo"
            }""".trimMargin()
        )
        val content = bestillingsordning_v2.transform(payload).content
        content.shouldContain("tidsstempel" to "AUTO")
        content.shouldContain("produkter" to setOf("123"))
        content.shouldContain("produkter_ikke_pa_bestillingsordning" to setOf("123"))
        content.shouldContain("bruker_har_hjelpemidler_fra_for" to true)
        content.shouldContain("bruker_har_infotrygd_vedtak_fra_for" to true)
        content.shouldContain("bruker_har_hotsak_vedtak_fra_for" to true)
        content.shouldContain("kommunenavn" to "Oslo")
    }

    @Test
    internal fun `payload transformed to bestillingsordning_v3 row`() {
        val payload = jsonMapper.readTree(
            """{
             "opprettet": "${LocalDateTime.now()}",
             "soknadid": "abc-123-def-456",
             "version": "v1.2.3",
             "kan_vere_bestilling": "true",
             "alle_hoved_produkter_pa_bestillingsordning": "true",
             "alle_tilbehor_pa_bestillingsordning": "true",
             "bruker_har_hjelpemidler_fra_for":  "null",
             "bruker_har_infotrygd_vedtak_fra_for":"false",
             "bruker_har_hotsak_vedtak_fra_for": "true",
             "levering_til_folkeregistrert_adresse": "true"
            }""".trimMargin()
        )
        val content = bestillingsordning_v3.transform(payload).content
        content.shouldContain("tidsstempel" to "AUTO")
        content.shouldContain("soknadid" to "abc-123-def-456")
        content.shouldContain("version" to "v1.2.3")
        content.shouldContain("alle_hoved_produkter_pa_bestillingsordning" to true)
        content.shouldContain("alle_tilbehor_pa_bestillingsordning" to true)
        content.shouldContain("bruker_har_hjelpemidler_fra_for" to false)
        content.shouldContain("bruker_har_infotrygd_vedtak_fra_for" to false)
        content.shouldContain("bruker_har_hotsak_vedtak_fra_for" to true)
        content.shouldContain("levering_til_folkeregistrert_adresse" to true)
    }

    @Test
    internal fun `payload transformed to hendelse_v1 row`() {
        val payload = jsonMapper.readTree(
            """{
            "opprettet": "${LocalDateTime.now()}",
            "navn": "navn",
            "kilde": "kilde",
            "data": {
                "someKey": "someValue"
            }
        }
            """.trimIndent()
        )
        val content = hendelse_v1.transform(payload).content
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to hendelse_v2 row`() {
        val payload = jsonMapper.readTree(
            """{
            "opprettet": "${LocalDateTime.now()}",
            "navn": "navn",
            "kilde": "kilde",
            "data": {
                "someKey": "someValue"
            }
        }
            """.trimIndent()
        )
        val content = hendelse_v2.transform(payload).content
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to hjelpemiddelstatistikk_v1 row`() {
        val payload = jsonMapper.readTree(
            """{
            "vedtaksdato": "${LocalDateTime.now()}",
            "vedtaksresultat": "Innvilget",
            "enhetsnummer": "Ukjent",
            "enhetsnavn": "Ukjent",
            "kommunenavn": "SULDAL",
            "brukers_alder": 78,
            "brukers_kjonn": "Mann",
            "brukers_funksjonsnedsettelser": ["bevegelse"],
            "hjelpemidler": [
              {
                "hmsnr": "243548",
                "produkt_id": "30389",
                "produktnavn": "Topro Skråbrett",
                "artikkel_id": "108383",
                "artikkelnavn": "Topro Skråbrett",
                "isokode": "18301505",
                "isotittel": "Terskeleliminatorer",
                "isokortnavn": "Terskeleliminator",
                "kategori": "Terskeleliminatorer og ramper",
                "avtalepost_id": "860",
                "avtaleposttittel": "Post 3: Terskeleliminator - påkjøring fra en side. Velegnet for utendørs bruk",
                "avtalepostrangering": 1
              }
            ]
          }
            """.trimIndent()
        )
        val content = hjelpemiddelstatistikk_v1.transform(payload).content
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to saksstatistikk_v1 row`() {
        val payload = jsonMapper.readTree(
            """{
            "sakId": "1",
            "behandlingId": "1",
            "saksbehandler": null,
            "saksbehandlerIdent": null
        }
            """.trimIndent()
        )
        val content = saksstatistikk_v1.transform(payload).content
        content shouldContain ("sak_id" to Hash.encode("1"))
        content shouldContain ("behandling_id" to Hash.encode("1"))
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to tilbakeforing_gosys_tilbakemelding_v1 row`() {
        val payload = jsonMapper.readTree(
            """{
            "saksnummer": "1",
            "opprettet": "${LocalDateTime.now()}",
            "enhetsnummer": "2970",
            "enhetsnavn": "NAV Test",
            "dokumentbeskrivelse": "foobar",
            "valgte_arsaker": [
                "arsak"
            ],
            "begrunnelse": "foobar"
        }
            """.trimIndent()
        )
        val content = tilbakeforing_gosys_tilbakemelding_v1.transform(payload).content
        assertSoftly {
            content shouldContain ("enhetsnummer" to "2970")
            content shouldContain ("enhetsnavn" to "NAV Test")
            content shouldContain ("dokumentbeskrivelse" to "foobar")
            content shouldContain ("valgte_arsaker" to setOf("arsak"))
            content shouldContain ("begrunnelse" to "foobar")
            content shouldContain ("tidsstempel" to "AUTO")
        }
    }
}
