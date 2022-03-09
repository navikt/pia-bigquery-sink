package no.nav.hjelpemidler.bigquery.sink

import com.fasterxml.jackson.module.kotlin.readValue

object Enhet {
    private val enheter = requireNotNull(javaClass.getResourceAsStream("/enheter.json")).use {
        jsonMapper.readValue<Map<String, String>>(it)
    }

    fun finnEnhetsnavn(enhetsnummer: String?): String = enheter.getOrDefault(enhetsnummer, enhetsnummer ?: "Ukjent")
}
