package no.nav.pia.bigquery.sink.schema

import com.google.cloud.bigquery.Clustering
import com.google.cloud.bigquery.RangePartitioning
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableDefinition
import com.google.cloud.bigquery.TimePartitioning

abstract class TableDefinitionBuilder<T : TableDefinition, B : TableDefinition.Builder<T, B>>(protected val builder: B) {
    fun schema(block: SchemaBuilder.() -> Unit): Unit = SchemaBuilder()
        .apply(block)
        .build()
        .let { builder.setSchema(it) }

    fun build(): T = builder.build()
}

class StandardTableDefinitionBuilder :
    TableDefinitionBuilder<StandardTableDefinition, StandardTableDefinition.Builder>(StandardTableDefinition.newBuilder()) {
    fun timePartitioning(
        type: TimePartitioning.Type,
        block: TimePartitioning.Builder.() -> Unit,
    ): StandardTableDefinition.Builder =
        TimePartitioning.newBuilder(type)
            .apply(block)
            .build()
            .let { builder.setTimePartitioning(it) }

    fun rangePartitioning(block: RangePartitioning.Builder.() -> Unit): StandardTableDefinition.Builder =
        RangePartitioning.newBuilder()
            .apply(block)
            .build()
            .let { builder.setRangePartitioning(it) }

    fun clustering(block: Clustering.Builder.() -> Unit): StandardTableDefinition.Builder =
        Clustering.newBuilder()
            .apply(block)
            .build()
            .let { builder.setClustering(it) }
}

fun standardTableDefinition(block: StandardTableDefinitionBuilder.() -> Unit): StandardTableDefinition =
    StandardTableDefinitionBuilder()
        .apply(block)
        .build()
