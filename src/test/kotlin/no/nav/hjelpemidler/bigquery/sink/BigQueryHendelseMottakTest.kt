package no.nav.hjelpemidler.bigquery.sink

import io.mockk.mockk
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.UUID

internal class BigQueryHendelseMottakTest {

    private val bigQueryServiceMock: BigQueryService = mockk(relaxed = true)
    private val rapid = TestRapid().also { rapidsConnection ->
        BigQueryHendelseMottak(rapidsConnection, bigQueryServiceMock)
    }

    @BeforeEach
    internal fun setUp() {
        rapid.reset()
    }

    @Test
    internal fun `event content is saved in BigQuery`() {
        rapid.sendTestMessage()
    }

    private fun TestRapid.sendTestMessage() = sendTestMessage(
        """
            {
              "eventId": "${UUID.randomUUID()}",
              "eventName": "hm-bigquery-sink-hendelse",
              "schemaId": "test_v1",
              "payload": {},
            }
        """.trimIndent()
    )
}
