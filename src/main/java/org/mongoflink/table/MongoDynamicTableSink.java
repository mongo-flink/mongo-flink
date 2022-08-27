package org.mongoflink.table;

import org.mongoflink.config.MongoConnectorOptions;
import org.mongoflink.serde.DocumentSerializer;
import org.mongoflink.serde.table.RowDataDocumentSerializer;
import org.mongoflink.sink.MongoSink;

import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class MongoDynamicTableSink implements DynamicTableSink {

    private final ResolvedSchema tableSchema;

    private final DocumentSerializer<RowData> serializer;
    private final MongoConnectorOptions options;

    public MongoDynamicTableSink(MongoConnectorOptions options, ResolvedSchema resolvedSchema) {
        this.tableSchema = resolvedSchema;
        serializer =
                new RowDataDocumentSerializer(tableSchema.toPhysicalRowDataType().getLogicalType());
        this.options = options;
    }

    @Override
    public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.DELETE)
                .addContainedKind(RowKind.UPDATE_AFTER)
                .build();
    }

    @Override
    public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
        return SinkProvider.of(new MongoSink<>(serializer, options));
    }

    @Override
    public DynamicTableSink copy() {
        return new MongoDynamicTableSink(options, tableSchema);
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Table Sink";
    }
}
