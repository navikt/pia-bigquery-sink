package no.nav.pia.bigquery.sink

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.mockk.mockk
import io.mockk.verify
import no.nav.pia.bigquery.sink.schema.SchemaDefinition
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import kotlin.test.assertFailsWith

internal class BigQueryHendelseMottakTest {
    private val bigQueryServiceMock: BigQueryService = mockk(relaxed = true)
    private val bigQueryHendelseMottak = BigQueryHendelseMottak(bigQueryServiceMock)

    @Test
    internal fun `event skal lagres i BigQuery`() {
        val payload = ObjectMapper().readValue(iaSakHendelseString, JsonNode::class.java)
        bigQueryHendelseMottak.onPacket(SchemaDefinition.Id.of("ia-sak-v1"), payload)
        verify { bigQueryServiceMock.insert(any(), any()) }
    }

    @Test
    internal fun `event med feil schemaid skal ikke lagres i BigQuery`() {
        val payload = ObjectMapper().readValue(iaSakHendelseString, JsonNode::class.java)
        assertFailsWith<IllegalStateException> {
            bigQueryHendelseMottak.onPacket(SchemaDefinition.Id.of("ukjent_schema-v1"), payload)
        }
    }

    private val iaSakHendelseString = """
            {
             "saksnummer": "123456789",
             "orgnr": "123456789",
             "eierAvSak": "N123456",
             "endretAvHendelseId": "123456789",
             "status": "VI_BISTÅR",
             "opprettetTidspunkt": "${LocalDateTime.now()}",
             "endretTidspunkt": "${LocalDateTime.now()}"
            }
        """.trimIndent()
}
