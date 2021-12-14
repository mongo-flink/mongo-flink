package org.mongoflink.source.reader;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsAddition;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;
import org.bson.Document;
import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.source.split.MongoRecords;
import org.mongoflink.source.split.MongoSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Queue;

/**
 * Reader that is responsible fetching records from MongoDB split by split.
 **/
public class MongoSplitReader implements SplitReader<Document, MongoSplit>{

    @Nullable
    private MongoSplit currentSplit;

    private final Queue<MongoSplit> pendingSplits;

    private final MongoClientProvider clientProvider;

    @Nullable
    private transient MongoCursor<Document> cursor;

    private int offset = 0;

    private int fetchSize = DEFAULT_FETCH_SIZE;

    private static final int DEFAULT_FETCH_SIZE = 200;

    private static final Logger LOG = LoggerFactory.getLogger(MongoSplitReader.class);

    public MongoSplitReader(MongoClientProvider clientProvider) {
        this.clientProvider = clientProvider;
        this.pendingSplits = Queues.newArrayDeque();
    }

    @Override
    public RecordsWithSplitIds<Document> fetch() throws IOException {
        prepareRead();

        Preconditions.checkNotNull(currentSplit);
        Preconditions.checkNotNull(cursor);

        List<Document> documents = Lists.newArrayList();
        while (documents.size() < fetchSize && cursor.hasNext()) {
            documents.add(cursor.next());
        }
        offset += documents.size();
        if (cursor.hasNext()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Fetched {} records from split {}, current offset: {}", documents.size(), currentSplit, offset);
            }
            return MongoRecords.forRecords(currentSplit.splitId(), documents);
        } else {
            String splitId = currentSplit.splitId();
            closeCurrentSplit();
            return MongoRecords.finishedSplit(splitId, documents);
        }
    }

    @Override
    public void handleSplitsChanges(SplitsChange<MongoSplit> splitChange) {
        if (!(splitChange instanceof SplitsAddition)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "The SplitChange type of %s is not supported.",
                            splitChange.getClass()));
        }
        LOG.info("Handling split change {}", splitChange);
        pendingSplits.addAll(splitChange.splits());
    }

    @Override
    public void wakeUp() {
        // mongo queries should fast, we don't need to interrupt the fetch loop
    }

    @Override
    public void close() throws Exception {
        if (cursor != null) {
            cursor.close();
        }
    }

    private void prepareRead() throws IOException {
        if (cursor != null) {
            // current split is in-progress
            return;
        }

        currentSplit = pendingSplits.poll();
        if (currentSplit == null) {
            throw new IOException("No more splits can be read.");
        }
        LOG.info("Prepared to read split {}", currentSplit.splitId());


        offset = 0;

        FindIterable<Document> rs =
                clientProvider.getDefaultCollection()
                        .find(currentSplit.getQuery())
                        .projection(currentSplit.getProjection())
                        .batchSize(fetchSize);
        cursor = rs.iterator();
    }

    private void closeCurrentSplit() {
        Preconditions.checkNotNull(currentSplit);
        LOG.info("Finished reading split {}.", currentSplit.splitId());
        currentSplit = null;

        Preconditions.checkNotNull(cursor);
        cursor.close();
        cursor = null;
    }
}
