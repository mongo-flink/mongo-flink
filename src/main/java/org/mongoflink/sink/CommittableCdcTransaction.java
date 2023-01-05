package org.mongoflink.sink;

import org.mongoflink.bson.CdcDocument;

import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.*;

import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple implementation of Mongo transaction body, which supports insertions, upserts, and
 * deletes.
 */
public class CommittableCdcTransaction extends CommittableTransaction {

    private final String[] upsertKeys;
    private final UpdateOptions updateOptions = new UpdateOptions();
    private final BulkWriteOptions bulkWriteOptions = new BulkWriteOptions();

    public CommittableCdcTransaction(
            MongoCollection<Document> collection, List<Document> documents, String[] upsertKeys) {
        super(collection, documents);
        this.upsertKeys = upsertKeys;
        updateOptions.upsert(true);
        bulkWriteOptions.ordered(true);
    }

    @Override
    public Integer execute() {
        List<WriteModel<Document>> writes =
                new ArrayList<>(); // interleave upserts and deletes to preserve order of operations
        for (Document document : bufferedDocuments) {
            List<Bson> filters = new ArrayList<>(upsertKeys.length);
            for (String upsertKey : upsertKeys) {
                Object o = document.get(upsertKey);
                Bson eq = Filters.eq(upsertKey, o);
                filters.add(eq);
            }
            Bson filter = Filters.and(filters);
            WriteModel<Document> model;
            if (((CdcDocument) document).isDelete()) {
                model = new DeleteOneModel<>(filter);
            } else {
                Document update = new Document();
                update.append("$set", document);
                model = new UpdateOneModel<>(filter, update, updateOptions);
            }
            writes.add(model);
        }

        BulkWriteResult bulkWriteResult = collection.bulkWrite(writes, bulkWriteOptions);

        return bulkWriteResult.getUpserts().size() + bulkWriteResult.getInsertedCount();
    }
}
