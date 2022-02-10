package no.nav.hjelpemidler.bigquery.sink.schema

import io.kotest.assertions.assertSoftly
import io.kotest.matchers.maps.shouldContain
import no.nav.hjelpemidler.bigquery.sink.jsonMapper
import org.junit.jupiter.api.Test
import java.time.LocalDateTime

internal class SchemaTest {

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
    internal fun `payload transformed to tilbakeforing_gosys_tilbakemelding_v1 row`() {
        val payload = jsonMapper.readTree("""{
            "saksnummer": "saksnummer",
            "opprettet": "${LocalDateTime.now()}",
            "enhetsnummer": "enhetsnummer",
            "enhetsnavn": "enhetsnavn",
            "dokument_beskrivelse": "dokument_beskrivelse",
            "tilbakemelding_valgte_arsaker": [
                "arsak"
            ],
            "tilbakemelding_begrunnelse": "tilbakemelding_begrunnelse"
        }""".trimIndent())
        val content = tilbakeforing_gosys_tilbakemelding_v1.transform(payload).content
        assertSoftly {
            content shouldContain ("saksnummer" to "saksnummer")
            content shouldContain ("enhetsnummer" to "enhetsnummer")
            content shouldContain ("enhetsnavn" to "enhetsnavn")
            content shouldContain ("dokument_beskrivelse" to "dokument_beskrivelse")
            content shouldContain ("tilbakemelding_valgte_arsaker" to setOf("arsak"))
            content shouldContain ("tilbakemelding_begrunnelse" to "tilbakemelding_begrunnelse")
            content shouldContain ("tidsstempel" to "AUTO")
        }
    }
}
