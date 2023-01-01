package org.mongoflink.sink;

import org.mongoflink.internal.connection.MongoClientProvider;

import org.apache.flink.api.connector.sink.Committer;

import com.mongodb.*;
import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * MongoCommitter flushes data to MongoDB in a transaction. Due to MVCC implementation of MongoDB, a
 * transaction is not recommended to be large.
 */
public class MongoCommitter implements Committer<DocumentBulk> {

    private final MongoClient client;

    private final MongoCollection<Document> collection;

    private final ClientSession session;

    private static final Logger LOGGER = LoggerFactory.getLogger(MongoCommitter.class);

    private TransactionOptions txnOptions =
            TransactionOptions.builder()
                    .readPreference(ReadPreference.primary())
                    .readConcern(ReadConcern.LOCAL)
                    .writeConcern(WriteConcern.MAJORITY)
                    .build();

    private final boolean enableUpsert;
    private final String[] upsertKeys;

    private static final long TRANSACTION_TIMEOUT_MS = 60_000L;

    private static final int DUPLICATE_KEY_ERROR_CODE = 11000;

    public MongoCommitter(MongoClientProvider clientProvider) {
        this(clientProvider, false, new String[] {});
    }

    public MongoCommitter(
            MongoClientProvider clientProvider, boolean enableUpsert, String[] upsertKeys) {
        this.client = clientProvider.getClient();
        this.collection = clientProvider.getDefaultCollection();
        this.session = client.startSession();
        this.enableUpsert = enableUpsert;
        this.upsertKeys = upsertKeys;
    }

    @Override
    public List<DocumentBulk> commit(List<DocumentBulk> committables) throws IOException {
        List<DocumentBulk> failedBulk = new ArrayList<>();

        for (DocumentBulk bulk : committables) {
            if (bulk.getDocuments().size() > 0) {
                CommittableTransaction transaction;
                if (enableUpsert) {
                    transaction =
                            new CommittableUpsertTransaction(
                                    collection, bulk.getDocuments(), upsertKeys);
                } else {
                    transaction = new CommittableTransaction(collection, bulk.getDocuments());
                }
                try {
                    int insertedDocs = session.withTransaction(transaction, txnOptions);

                    LOGGER.info(
                            "Inserted {} documents into collection {}.",
                            insertedDocs,
                            collection.getNamespace());
                } catch (MongoBulkWriteException e) {
                    // for insertions, ignore duplicate key errors in case txn was already committed
                    // but client was not aware of it

                    // NOTE: upserts are idempotent in mongo, so no exceptions need to be caught for
                    // redundant upserts
                    for (WriteError err : e.getWriteErrors()) {
                        if (err.getCode() != DUPLICATE_KEY_ERROR_CODE) {
                            // for now, simply requeueing records when a write error
                            // other than duplicate key is encountered. In some cases, this may
                            // result in annoying error looping, but should not cause data loss.
                            // TODO: handle specific write errors as necessary
                            LOGGER.error(
                                    String.format(
                                            "Mongo write error – requeueing %d records",
                                            bulk.size()),
                                    e);
                            failedBulk.add(bulk);
                            break;
                        }
                    }
                    LOGGER.warn("Ignoring duplicate records");
                } catch (Exception e) {
                    // save to a new list that would be retried
                    LOGGER.error(
                            String.format(
                                    "Failed to commit with Mongo transaction – requeueing %d records",
                                    bulk.size()),
                            e);
                    failedBulk.add(bulk);
                }
                // TODO: [DE-3303] if needed, catch duplicate delete errors
            }
        }
        return failedBulk;
    }

    @Override
    public void close() throws Exception {
        long deadline = System.currentTimeMillis() + TRANSACTION_TIMEOUT_MS;
        while (session.hasActiveTransaction() && System.currentTimeMillis() < deadline) {
            // wait for active transaction to finish or timeout
            Thread.sleep(5_000L);
        }
        session.close();
        client.close();
    }
}
