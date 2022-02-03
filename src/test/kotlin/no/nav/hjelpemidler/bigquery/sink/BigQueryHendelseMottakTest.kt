package no.nav.hjelpemidler.bigquery.sink

import io.mockk.mockk
import no.nav.helse.rapids_rivers.testsupport.TestRapid
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.time.LocalDateTime
import java.util.UUID

internal class BigQueryHendelseMottakTest {

    private val bigQueryClientMock: BigQueryClient = mockk(relaxed = true)
    private val rapid = TestRapid().also { rapidsConnection ->
        BigQueryHendelseMottak(rapidsConnection, bigQueryClientMock)
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
              "eventName": "hm-bigquery-hendelse",
              "opprettet": "${LocalDateTime.now()}",
            }
        """.trimIndent()
    )
}
