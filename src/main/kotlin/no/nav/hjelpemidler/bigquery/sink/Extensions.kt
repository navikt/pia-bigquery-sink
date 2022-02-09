package no.nav.hjelpemidler.bigquery.sink

import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.TableId

fun DatasetId.table(table: String): TableId = TableId.of(project, dataset, table)

fun Map<String, String>.toStructEntries(
    nameKey: String = "navn",
    valueKey: String = "verdi",
): List<Map<String, String>> = map {
    mapOf(nameKey to it.key, valueKey to it.value)
}
