package mongoflink.sink;

import mongoflink.config.SinkConfiguration;
import mongoflink.config.SinkConfigurationFactory;
import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.internal.connection.MongoColloctionProviders;
import mongoflink.serde.DocumentSerializer;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

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

    private DocumentSerializer<IN> serializer;

    private final SinkConfiguration configuration;

    public MongoSink(String connectionString,
                     String database,
                     String collection,
                     DocumentSerializer<IN> serializer,
                     Properties properties) {
        this.configuration = SinkConfigurationFactory.fromProperties(properties);
        this.serializer = serializer;
        this.clientProvider =
                MongoColloctionProviders
                        .getBuilder()
                        .connectionString(connectionString)
                        .database(database)
                        .collection(collection).build();
    }

    @Override
    public SinkWriter<IN, DocumentBulk, DocumentBulk> createWriter(InitContext initContext, List<DocumentBulk> states)
            throws IOException {
        MongoBulkWriter<IN> writer = new MongoBulkWriter<>(clientProvider, serializer, configuration);
        writer.initializeState(states);
        return writer;
    }

    @Override
    public Optional<Committer<DocumentBulk>> createCommitter() throws IOException {
        if (configuration.isTransactional()) {
            return Optional.of(new MongoCommitter(clientProvider));
        }
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<DocumentBulk, Void>> createGlobalCommitter() throws IOException {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getCommittableSerializer() {
        return Optional.of(DocumentBulkSerializer.INSTANCE);
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<DocumentBulk>> getWriterStateSerializer() {
        return Optional.of(DocumentBulkSerializer.INSTANCE);
    }
}
