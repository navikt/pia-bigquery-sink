package no.nav.hjelpemidler.bigquery.sink.registry

import com.fasterxml.jackson.databind.JsonNode
import com.google.cloud.bigquery.InsertAllRequest.RowToInsert
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning
import no.nav.hjelpemidler.bigquery.sink.Enhet
import no.nav.hjelpemidler.bigquery.sink.asMap
import no.nav.hjelpemidler.bigquery.sink.schema.FieldBuilder
import no.nav.hjelpemidler.bigquery.sink.schema.SchemaDefinition
import no.nav.hjelpemidler.bigquery.sink.schema.standardTableDefinition

val hjelpemiddelstatistikk_v1 = object : SchemaDefinition {
    override val schemaId: SchemaDefinition.Id = SchemaDefinition.Id(
        name = "hjelpemiddelstatistikk",
        version = 1,
    )

    override fun define(): TableDefinition = standardTableDefinition {
        schema {
            datetime("vedtaksdato") {
                required()
                description("Dato og klokkeslett for vedtak")
            }
            string("vedtaksresultat") {
                required()
                description("Utfall av saksbehandling")
            }
            string("enhetsnummer") {
                required()
                description("Hjelpemiddelsentralens enhetsnummer")
            }
            string("enhetsnavn") {
                required()
                description("Hjelpemiddelsentralens navn")
            }
            string("kommunenavn") {
                required()
                description("Brukers bostedskommune")
            }
            integer("brukers_alder") {
                required()
                description("Brukers alder ved vedtaksdato")
            }
            string("brukers_kjonn") {
                required()
                description("Brukers kjønn")
            }
            string("brukers_funksjonsnedsettelser") {
                repeated()
                description("Brukers funksjonsnedsettelser")
            }
            struct("hjelpemidler") {
                repeated()
                description("Hjelpemidlene det er søkt om")
                hjelpemiddelfelter()
            }
            struct("tilbehor") {
                repeated()
                description("Tilbehørene det er søkt om")
                hjelpemiddelfelter()
            }
            timestamp("tidsstempel") {
                required()
                description("Tidsstempel for lagring av raden")
            }
        }
        timePartitioning(TimePartitioning.Type.MONTH) {
            setField("vedtaksdato")
        }
        clustering {
            setFields(listOf("vedtaksdato"))
        }
    }

    override fun transform(payload: JsonNode): RowToInsert = payload
        .asMap()
        .plus("enhetsnavn" to Enhet.finnEnhetsnavn(payload.enhetsnummer()))
        .plus("tidsstempel" to "AUTO")
        .toRowToInsert()

    private fun JsonNode.enhetsnummer() = get("enhetsnummer").asText()

    private fun FieldBuilder.hjelpemiddelfelter() = subFields {
        string("hmsnr") {
            nullable()
            description("Hjelpemiddelets HMS-nr.")
        }
        string("produkt_id") {
            required()
            description("Produktseriens ID")
        }
        string("produktnavn") {
            required()
            description("Produktseriens navn")
        }
        string("artikkel_id") {
            required()
            description("Hjelpemiddelets ID")
        }
        string("artikkelnavn") {
            required()
            description("Hjelpemiddelets navn")
        }
        string("isokode") {
            required()
            description("Hjelpemiddelets ISO-klassifisering (kode)")
        }
        string("isotittel") {
            required()
            description("Hjelpemiddelets ISO-klassifisering (navn)")
        }
        string("isokortnavn") {
            nullable()
            description("Hjelpemiddelets ISO-klassifisering (kort)")
        }
        string("kategori") {
            nullable()
            description("Hjelpemiddelets kategori")
        }
        string("avtalepost_id") {
            nullable()
            description("Rammeavtalepostens ID")
        }
        string("avtaleposttittel") {
            nullable()
            description("Rammeavtalepostens navn")
        }
        string("avtalepostrangering") {
            nullable()
            description("Rammeavtalepostens rangering")
        }
        string("kilde") {
            required()
            description("Søknad / Ordre / Søknad og ordre")
        }
    }
}
