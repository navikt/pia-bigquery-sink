package no.nav.hjelpemidler.bigquery.sink

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test

internal class EnhetTest {

    @Test
    internal fun `finner enhetsnavn som finnes`() {
        Enhet.finnEnhetsnavn("2970") shouldBe "NAV IKT DRIFT"
    }

    @Test
    internal fun `finner enhetsnavn som ikke finnes`() {
        Enhet.finnEnhetsnavn("1337") shouldBe "1337"
    }

    @Test
    internal fun `finner enhetsnavn som er null`() {
        Enhet.finnEnhetsnavn(null) shouldBe "Ukjent"
    }
}
