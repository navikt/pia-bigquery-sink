package no.nav.hjelpemidler.bigquery.sink.registry

import com.google.cloud.bigquery.DatasetId
import io.kotest.assertions.assertSoftly
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.shouldBe
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
    internal fun `payload transformed to hendelse_v1 row`() {
        val payload = jsonMapper.readTree("""{
            "opprettet": "${LocalDateTime.now()}",
            "navn": "navn",
            "kilde": "kilde",
            "data": {
                "someKey": "someValue"
            }
        }""".trimIndent())
        val content = hendelse_v1.transform(payload).content
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to hendelse_v2 row`() {
        val payload = jsonMapper.readTree("""{
            "opprettet": "${LocalDateTime.now()}",
            "navn": "navn",
            "kilde": "kilde",
            "data": {
                "someKey": "someValue"
            }
        }""".trimIndent())
        val content = hendelse_v2.transform(payload).content
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to hjelpemiddelstatistikk_v1 row`() {
        val payload = jsonMapper.readTree("""{
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
        """.trimIndent())
        val content = hjelpemiddelstatistikk_v1.transform(payload).content
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to saksstatistikk_v1 row`() {
        val payload = jsonMapper.readTree("""{
            "sakId": "1",
            "behandlingId": "1",
            "saksbehandler": null,
            "saksbehandlerIdent": null
        }""".trimIndent())
        val content = saksstatistikk_v1.transform(payload).content
        content shouldContain ("sak_id" to "1")
        content shouldContain ("behandling_id" to "1")
        content shouldContain ("tidsstempel" to "AUTO")
    }

    @Test
    internal fun `payload transformed to tilbakeforing_gosys_tilbakemelding_v1 row`() {
        val payload = jsonMapper.readTree("""{
            "saksnummer": "1",
            "opprettet": "${LocalDateTime.now()}",
            "enhetsnummer": "2970",
            "enhetsnavn": "NAV Test",
            "dokumentbeskrivelse": "foobar",
            "valgte_arsaker": [
                "arsak"
            ],
            "begrunnelse": "foobar"
        }""".trimIndent())
        val content = tilbakeforing_gosys_tilbakemelding_v1.transform(payload).content
        assertSoftly {
            content shouldContain ("saksnummer" to "1")
            content shouldContain ("enhetsnummer" to "2970")
            content shouldContain ("enhetsnavn" to "NAV Test")
            content shouldContain ("dokumentbeskrivelse" to "foobar")
            content shouldContain ("valgte_arsaker" to setOf("arsak"))
            content shouldContain ("begrunnelse" to "foobar")
            content shouldContain ("tidsstempel" to "AUTO")
        }
    }
}
