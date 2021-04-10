package mongoflink.sink;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import mongoflink.config.Configuration;
import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.serde.DocumentSerializer;

import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.bson.Document;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

/**
 * Writer for MongoDB sink.
 **/
public class MongoBulkWriter<IN> implements SinkWriter<IN, DocumentBulk, DocumentBulk> {

    private final MongoClientProvider collectionProvider;

    private transient MongoCollection<Document> collection;

    private DocumentBulk currentBulk;

    private final List<DocumentBulk> pendingBulks = new ArrayList<>();

    private DocumentSerializer<IN> serializer;

    //TODO: implement time-based and size-based flush
    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture scheduledFuture;

    private final long maxSize;

    private final boolean flushOnCheckpoint;

    private final RetryPolicy retryPolicy = new RetryPolicy(3, 1000L);

    private transient volatile boolean closed = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoBulkWriter.class);

    public MongoBulkWriter(MongoClientProvider collectionProvider,
                           DocumentSerializer<IN> serializer,
                           Configuration configuration) {
        this.collectionProvider = collectionProvider;
        this.serializer = serializer;
        this.maxSize = configuration.getBulkFlushSize();
        this.flushOnCheckpoint = configuration.isFlushOnCheckpoint();
        this.currentBulk = new DocumentBulk(maxSize);
        this.scheduler =
                Executors.newScheduledThreadPool(
                        1, new ExecutorThreadFactory("mongodb-bulk-writer"));
    }

    public void initializeState(List<DocumentBulk> recoveredBulks) {
        collection = collectionProvider.getDefaultCollection();
        for (DocumentBulk bulk: recoveredBulks) {
            for (Document document: bulk.getDocuments()) {
                rollBulkIfNeeded();
                currentBulk.add(document);
            }
        }
    }

    @Override
    public void write(IN o, Context context) throws IOException {
        rollBulkIfNeeded();
        currentBulk.add(serializer.serialize(o));
    }

    @Override
    public List<DocumentBulk> prepareCommit(boolean flush) throws IOException {
        if (flushOnCheckpoint || flush) {
            rollBulkIfNeeded(true);
        }
        return pendingBulks;
    }

    @Override
    public List<DocumentBulk> snapshotState() throws IOException {
        List<DocumentBulk> inProgressAndPendingBulks = new ArrayList<>(1);
        inProgressAndPendingBulks.add(currentBulk);
        inProgressAndPendingBulks.addAll(pendingBulks);
        pendingBulks.clear();
        return inProgressAndPendingBulks;
    }

    @Override
    public void close() throws Exception {
        closed = true;
    }

    /**
     * Flush by non-transactional bulk write, which may result in data duplicates after multiple tries.
     * There may be concurrent flushes when concurrent checkpoints are enabled.
     */
    private synchronized void flush() {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            for (DocumentBulk bulk : pendingBulks) {
                do {
                    try {
                        // ordered, non-bypass mode
                        collection.insertMany(bulk.getDocuments());
                        pendingBulks.remove(bulk);
                        break;
                    } catch (MongoException e) {
                        // maybe partial failure
                        LOGGER.error("Failed to flush data to MongoDB", e);
                    }
                } while (!closed && retryPolicy.shouldBackoffRetry());
            }
        }
    }

    private void ensureConnection() {
        try {
            collection.listIndexes();
        } catch (MongoException e) {
            LOGGER.warn("Connection is not available, try to reconnect", e);
            collectionProvider.recreateClient();
        }
    }

    private void rollBulkIfNeeded() {
        rollBulkIfNeeded(false);
    }
    private void rollBulkIfNeeded(boolean force) {
        if (force || currentBulk.isFull()) {
            pendingBulks.add(currentBulk);
            currentBulk = new DocumentBulk(maxSize);
        }
    }

    @NotThreadSafe
    class RetryPolicy {

        private final long maxRetries;

        private final long backoffMillis;

        private long currentRetries = 0L;

        RetryPolicy(long maxRetries, long backoffMillis) {
            this.maxRetries = maxRetries;
            this.backoffMillis = backoffMillis;
        }

        boolean shouldBackoffRetry() {
            if (++currentRetries > maxRetries) {
                return false;
            } else {
                backoff();
                return true;
            }
        }

        private void backoff() {
            try {
                Thread.sleep(backoffMillis);
            } catch (InterruptedException e) {
                // exit backoff
            }
        }

        void reset() {
            currentRetries = 0L;
        }
    }
}
