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
