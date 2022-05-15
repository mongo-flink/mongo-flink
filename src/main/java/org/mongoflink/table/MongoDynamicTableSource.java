package org.mongoflink.table;

import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import org.apache.flink.table.api.TableColumn;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.connector.source.abilities.SupportsFilterPushDown;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.bson.BsonDocument;
import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.internal.connection.MongoColloctionProviders;
import org.mongoflink.serde.table.DocumentRowDataDeserializer;
import org.mongoflink.source.MongoSource;
import org.mongoflink.source.pushdown.MongoFilters;
import org.mongoflink.source.split.SamplingSplitStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

public class MongoDynamicTableSource implements ScanTableSource,
        SupportsProjectionPushDown, SupportsFilterPushDown {

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoDynamicTableSource.class);

    private final TableSchema physicalSchema;

    private final String connectString;
    private final String database;
    private final String collection;

    private List<ResolvedExpression> filters;

    /**
     * Provides the field index paths that should be used for a projection. The indices are 0-based
     * and support fields within (possibly nested) structures if this is enabled via {@link #supportsNestedProjection()}.
     *
     * <p>In the example mentioned in {@link SupportsProjectionPushDown}, this method would receive:
     *
     * <ul>
     *   <li>{@code [[2], [1]]} which is equivalent to {@code [["s"], ["r"]]} if {@link
     *       #supportsNestedProjection()} returns false.
     *   <li>{@code [[2], [1, 0]]} which is equivalent to {@code [["s"], ["r", "d"]]]} if {@link
     *       #supportsNestedProjection()} returns true.
     * </ul>
     * <p>
     * field index paths of all fields that must be present in the physically produced data
     */
    private int[][] projectedFieldIndexes;

    private String[] projectedFieldNames;

    public MongoDynamicTableSource(String connectString, String database, String collection,
                                   TableSchema physicalSchema) {
        this.connectString = connectString;
        this.database = database;
        this.collection = collection;
        this.physicalSchema = physicalSchema;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {

        MongoClientProvider clientProvider = MongoColloctionProviders
                .getBuilder()
                .connectionString(connectString)
                .database(database)
                .collection(collection)
                .build();

        BsonDocument mongoFilter = Filters.empty().toBsonDocument();
        if (filters != null) {
            mongoFilter = MongoFilters.build(filters);
        }

        SamplingSplitStrategy.Builder builder = SamplingSplitStrategy.builder()
                .setMatchQuery(mongoFilter)
                .setClientProvider(clientProvider);

        if (this.projectedFieldNames != null && this.projectedFieldNames.length > 0) {
            builder.setProjection(Projections.include(this.projectedFieldNames).toBsonDocument());
        }
        if (projectedFieldNames == null || projectedFieldNames.length < 1) {
            projectedFieldNames = physicalSchema.getFieldNames();
        }
        DataType[] projectedFieldType = new DataType[projectedFieldNames.length];
        for (int i = 0; i < projectedFieldNames.length; i++) {
            projectedFieldType[i] = physicalSchema.getFieldDataType(projectedFieldNames[i]).get();
        }

        DocumentRowDataDeserializer deserializer = new DocumentRowDataDeserializer(this.projectedFieldNames, projectedFieldType);
        return SourceProvider.of(new MongoSource<>(clientProvider, deserializer, builder.build()));
    }

    @Override
    public DynamicTableSource copy() {
        MongoDynamicTableSource mongoDynamicTableSource = new MongoDynamicTableSource(connectString, database, collection, physicalSchema);
        mongoDynamicTableSource.projectedFieldIndexes = this.projectedFieldIndexes;
        mongoDynamicTableSource.filters = this.filters;
        mongoDynamicTableSource.projectedFieldNames = this.projectedFieldNames;
        return mongoDynamicTableSource;
    }

    @Override
    public String asSummaryString() {
        return "MongoDB Table Source";
    }

    @Override
    public Result applyFilters(List<ResolvedExpression> filters) {
        this.filters = new ArrayList<>(filters);
        return Result.of(new ArrayList<>(filters), new ArrayList<>(filters));
    }

    @Override
    public boolean supportsNestedProjection() {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void applyProjection(int[][] projectedFields) {
        this.projectedFieldIndexes = projectedFields;
        projectionFieldNames();
    }

    private void projectionFieldNames() {
        int[] fields = projectedFieldIndexes == null ? IntStream.range(0, physicalSchema.getFieldCount()).toArray()
                : Arrays.stream(projectedFieldIndexes).mapToInt(arr -> arr[0]).toArray();
        String[] fieldNames = physicalSchema.getFieldNames();
        this.projectedFieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            this.projectedFieldNames[i] = fieldNames[fields[i]];
        }
    }


}

