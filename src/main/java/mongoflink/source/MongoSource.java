package mongoflink.source;

import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.serde.DocumentDeserializer;
import mongoflink.source.enumerator.MongoSplitEnumerator;
import mongoflink.source.reader.MongoReader;
import mongoflink.source.split.ListMongoSplitSerializer;
import mongoflink.source.split.MongoSplit;
import mongoflink.source.split.MongoSplitSerializer;
import mongoflink.source.split.MongoSplitStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.util.List;

/**
 * MongoSource for bounded scenarios.
 **/
public class MongoSource<T> implements Source<T, MongoSplit, List<MongoSplit>> {

    private MongoClientProvider clientProvider;

    private DocumentDeserializer<T> deserializer;

    private MongoSplitStrategy splitStrategy;

    public MongoSource(MongoClientProvider clientProvider,
                       DocumentDeserializer<T> deserializer,
                       MongoSplitStrategy splitStrategy) {
        this.clientProvider = clientProvider;
        this.deserializer = deserializer;
        this.splitStrategy = splitStrategy;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<T, MongoSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new MongoReader<>(readerContext, clientProvider, deserializer);
    }

    @Override
    public SplitEnumerator<MongoSplit, List<MongoSplit>> createEnumerator(
            SplitEnumeratorContext<MongoSplit> enumContext) throws Exception {
        return new MongoSplitEnumerator(enumContext, clientProvider, splitStrategy);
    }

    @Override
    public SplitEnumerator<MongoSplit, List<MongoSplit>> restoreEnumerator(
            SplitEnumeratorContext<MongoSplit> enumContext,
            List<MongoSplit> checkpointedSplits) throws Exception {
        return new MongoSplitEnumerator(enumContext, clientProvider, splitStrategy, checkpointedSplits);
    }

    @Override
    public SimpleVersionedSerializer<MongoSplit> getSplitSerializer() {
        return MongoSplitSerializer.INSTANCE;
    }

    @Override
    public SimpleVersionedSerializer<List<MongoSplit>> getEnumeratorCheckpointSerializer() {
        return ListMongoSplitSerializer.INSTANCE;
    }
}
