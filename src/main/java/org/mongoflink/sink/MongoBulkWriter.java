package org.mongoflink.sink;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;

import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.mongoflink.config.MongoConnectorOptions;
import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.serde.DocumentSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Writer for MongoDB sink.
 **/
public class MongoBulkWriter<IN> implements SinkWriter<IN, DocumentBulk, DocumentBulk> {

    private final MongoClientProvider collectionProvider;

    private transient MongoCollection<Document> collection;

    private final ConcurrentLinkedQueue<Document> currentBulk = new ConcurrentLinkedQueue<>();

    private final List<DocumentBulk> pendingBulks = new ArrayList<>();

    private DocumentSerializer<IN> serializer;

    private transient ScheduledExecutorService scheduler;

    private transient ScheduledFuture scheduledFuture;

    private transient volatile Exception flushException;

    private final long maxSize;

    private final boolean flushOnCheckpoint;
    private final boolean transactionEnable;

    private final boolean upsertEnable;
    private final String[] upsertKeys;

    private final RetryPolicy retryPolicy = new RetryPolicy(3, 1000L);

    private final transient MongoConnectorOptions options;

    private transient volatile boolean closed = false;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoBulkWriter.class);

    public MongoBulkWriter(MongoClientProvider collectionProvider,
                           DocumentSerializer<IN> serializer,
                           MongoConnectorOptions options) {
        this.upsertEnable = options.isUpsertEnable();
        this.upsertKeys = options.getUpsertKey();
        this.collectionProvider = collectionProvider;
        this.serializer = serializer;
        this.maxSize = options.getFlushSize();
        this.flushOnCheckpoint = options.getFlushOnCheckpoint();
        this.transactionEnable = options.isTransactionEnable();
        this.options = options;
    }

    private void startSchedule() {
        if (!flushOnCheckpoint && this.options.getFlushInterval().getSeconds() > 0) {
            this.scheduler =
                    Executors.newScheduledThreadPool(
                            1, new ExecutorThreadFactory("mongodb-bulk-writer"));
            this.scheduledFuture =
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                                synchronized (MongoBulkWriter.this) {
                                    if (!closed) {
                                        try {
                                            rollBulkIfNeeded(true);
                                            flush();
                                        } catch (Exception e) {
                                            flushException = e;
                                        }
                                    }
                                }
                            },
                            options.getFlushInterval().get(ChronoUnit.SECONDS),
                            options.getFlushInterval().get(ChronoUnit.SECONDS),
                            TimeUnit.SECONDS);
        }
    }


    public void initializeState(List<DocumentBulk> recoveredBulks) {
        collection = collectionProvider.getDefaultCollection();
        for (DocumentBulk bulk : recoveredBulks) {
            for (Document document : bulk.getDocuments()) {
                currentBulk.add(document);
                rollBulkIfNeeded();
            }
        }
        startSchedule();
    }

    @Override
    public void write(IN o, Context context) throws IOException {
        checkFlushException();

        currentBulk.add(serializer.serialize(o));
        rollBulkIfNeeded();
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
        synchronized (this) {
            DocumentBulk x = new DocumentBulk();
            for (Document document : currentBulk) {
                x.add(document);
            }

            inProgressAndPendingBulks.add(x);
            inProgressAndPendingBulks.addAll(pendingBulks);
        }

        return inProgressAndPendingBulks;
    }

    @Override
    public void close() throws Exception {
        if (!transactionEnable) {
            synchronized (this) {
                if (!closed) {
                    try {
                        rollBulkIfNeeded(true);
                        flush();
                    } catch (Exception e) {
                        flushException = e;
                    }
                }
            }
        }
        closed = true;
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
        }
        if (scheduler != null) {
            scheduler.shutdown();
        }
        if (collectionProvider != null) {
            collectionProvider.close();
        }
    }

    /**
     * Flush by non-transactional bulk write, which may result in data duplicates after multiple tries.
     * There may be concurrent flushes when concurrent checkpoints are enabled.
     * <p>
     * We manually retry write operations, because the driver doesn't support automatic retries for some MongoDB
     * setups (e.g. standalone instances). TODO: This should be configurable in the future.
     */
    private synchronized void flush() {
        if (!closed) {
            ensureConnection();
            retryPolicy.reset();
            Iterator<DocumentBulk> iterator = pendingBulks.iterator();
            if (this.upsertEnable) {
                flushUpsert(iterator);
            } else {
                while (iterator.hasNext()) {
                    DocumentBulk bulk = iterator.next();
                    do {
                        try {
                            // ordered, non-bypass mode
                            if (bulk.size() > 0) {
                                collection.insertMany(bulk.getDocuments());
                            }
                            iterator.remove();
                            break;
                        } catch (MongoException e) {
                            // maybe partial failure
                            LOGGER.error("Failed to flush data to MongoDB", e);
                        }
                    } while (!closed && retryPolicy.shouldBackoffRetry());
                }
            }
        }
    }

    private void flushUpsert(Iterator<DocumentBulk> iterator) {
        UpdateOptions updateOptions = new UpdateOptions();
        updateOptions.upsert(true);
        BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();
        bulkWriteOptions.ordered(true);
        while (iterator.hasNext()) {
            DocumentBulk bulk = iterator.next();
            do {
                try {
                    if (bulk.size() > 0) {
                        List<Document> documents = bulk.getDocuments();
                        List<UpdateOneModel<Document>> upserts = new ArrayList<>();
                        for (Document document : documents) {
                            List<Bson> filters = new ArrayList<>(upsertKeys.length);
                            for (String upsertKey : upsertKeys) {
                                Object o = document.get(upsertKey);
                                Bson eq = Filters.eq(upsertKey, o);
                                filters.add(eq);
                            }
                            Document update = new Document();
                            update.append("$set", document);
                            Bson filter = Filters.and(filters);
                            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<>(filter, update, updateOptions);
                            upserts.add(updateOneModel);
                        }
                        collection.bulkWrite(upserts, bulkWriteOptions);
                    }

                    iterator.remove();
                    break;
                } catch (MongoException e) {
                    // maybe partial failure
                    e.printStackTrace();
                    LOGGER.error("Failed to flush data to MongoDB", e);
                }
            } while (!closed && retryPolicy.shouldBackoffRetry());
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

    private synchronized void rollBulkIfNeeded(boolean force) {
        int size = currentBulk.size();

        if (force || size >= maxSize) {

            DocumentBulk x = new DocumentBulk(maxSize);
            while (size-- > 0) {
                if (x.size() >= maxSize) {
                    pendingBulks.add(x);
                    x = new DocumentBulk(maxSize);
                }

                x.add(currentBulk.poll());
            }

            pendingBulks.add(x);
        }
    }

    private void checkFlushException() throws IOException {
        if (flushException != null) {
            throw new IOException("Failed to flush records to MongoDB", flushException);
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
