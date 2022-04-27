package org.mongoflink.sink;

import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.mongoflink.config.MongoConnectorOptions;
import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.internal.connection.MongoColloctionProviders;
import org.mongoflink.serde.DocumentSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * <p> Flink sink connector for MongoDB. MongoSink supports transaction mode for MongoDB 4.2+ and non-transaction mode for
 * Mongo 3.0+. </p>
 *
 * <p> In transaction mode, all writes will be buffered in memory and committed to MongoDB in per-taskmanager
 * transactions on successful checkpoints, which ensures exactly-once semantics. </p>
 *
 * <p> In non-transaction mode, writes would be periodically flushed to MongoDB, which provides at-least-once semantics.
 * </p>
 **/
public class MongoSink<IN> implements Sink<IN, DocumentBulk, DocumentBulk, Void> {

    private final MongoClientProvider clientProvider;

    private final DocumentSerializer<IN> serializer;

    private final MongoConnectorOptions options;

    public MongoSink(DocumentSerializer<IN> serializer,
                     MongoConnectorOptions options) {
        this.options = options;
        this.serializer = serializer;
        this.clientProvider =
                MongoColloctionProviders
                        .getBuilder()
                        .connectionString(this.options.getConnectString())
                        .database(this.options.getDatabase())
                        .collection(this.options.getCollection()).build();
    }

    @Override
    public SinkWriter<IN, DocumentBulk, DocumentBulk> createWriter(InitContext initContext, List<DocumentBulk> states)
            throws IOException {
        MongoBulkWriter<IN> writer = new MongoBulkWriter<>(clientProvider, serializer, options);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<DocumentBulk>> createCommitter() throws IOException {
        if (options.isTransactionEnable()) {
            String[] upsertKeys = new String[]{};
            if (options.isUpsertEnable()) {
                upsertKeys = options.getUpsertKey();
            }
            return Optional.of(new MongoCommitter(clientProvider, options.isUpsertEnable(), upsertKeys));
        }
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<DocumentBulk, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getCommittableSerializer() {
        if (options.isTransactionEnable()) {
            return Optional.of(DocumentBulkSerializer.INSTANCE);
        }
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getWriterStateSerializer() {
        if (options.isTransactionEnable()) {
            return Optional.of(DocumentBulkSerializer.INSTANCE);
        }
        return Optional.empty();
    }

}
