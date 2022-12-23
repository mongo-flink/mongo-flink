package org.mongoflink.sink;

import com.mongodb.*;
import org.mongoflink.internal.connection.MongoClientProvider;

import org.apache.flink.api.connector.sink.Committer;

import com.mongodb.client.ClientSession;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

        int rand = new Random().nextInt(10 - 1 + 1) + 1;
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
//                    // test 1
//                    if (rand == 1) {
//                        System.out.println("***** TEST 1 *****");
//                        throw new IOException("test 1");
//                    }

                    int insertedDocs = session.withTransaction(transaction, txnOptions);

                    // test 2
                    if (rand == 2) {
                        System.out.println("***** TEST 2 *****");
                        throw new IOException("test 2");
                    }

                    LOGGER.info(
                            "Inserted {} documents into collection {}.",
                            insertedDocs,
                            collection.getNamespace());
                } catch (MongoBulkWriteException e) {
                    // ignore duplicate key errors in case txn was already committed but client was not aware of it
                    for (WriteError err : e.getWriteErrors()) {
                        if (err.getCode() != DUPLICATE_KEY_ERROR_CODE) {
                            LOGGER.error("Failed to commit with Mongo transaction.", e);
                            failedBulk.add(bulk);
                        }
                        // TODO: catch other write errors
                    }
                    LOGGER.warn("Ignoring duplicate records");
                } catch (Exception e) {
                    // save to a new list that would be retried
                    LOGGER.error("Failed to commit with Mongo transaction.", e);
                    failedBulk.add(bulk);
                }
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
