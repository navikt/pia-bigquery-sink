package no.nav.pia.bigquery.sink.schema

import com.google.cloud.bigquery.DatasetId

class Registry(
    val datasetId: DatasetId,
    private val registry: Map<SchemaDefinition.Id, SchemaDefinition>,
) : Map<SchemaDefinition.Id, SchemaDefinition> by registry {
    constructor(datasetId: DatasetId, vararg pairs: Pair<SchemaDefinition.Id, SchemaDefinition>) : this(
        datasetId,
        mapOf(*pairs),
    )
}
