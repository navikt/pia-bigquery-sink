package no.nav.hjelpemidler.bigquery.sink

import java.security.MessageDigest
import java.util.HexFormat

object Hash {
    private val defaultSuffix: String? = Config.getOrNull(BigQuery.hash_suffix)

    private val hexFormat: HexFormat = HexFormat.of()

    private fun ByteArray.toHex(): String = hexFormat.formatHex(this)

    private fun <T> digest(block: (MessageDigest) -> T): T = block(MessageDigest.getInstance("SHA-256"))

    fun encode(value: String, suffix: String? = defaultSuffix): String = when (value) {
        "" -> ""
        else -> digest {
            val input = listOfNotNull(value, suffix)
                .joinToString("_")
                .toByteArray()
            it.digest(input)
        }.toHex()
    }
}
