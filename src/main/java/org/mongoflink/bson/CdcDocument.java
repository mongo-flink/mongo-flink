package org.mongoflink.bson;

import org.bson.Document;

import java.util.stream.Collectors;

/**
 * CdcDocument is a bson Document with a delete flag used to indicate if a given Document in a
 * DocumentBulk represents a mongo delete operation.
 */
public class CdcDocument extends Document {
    private boolean isDelete;

    public CdcDocument(Document document) {
        // Note: field ordering within a document is not preserved when instantiating a CdcDocument
        super(
                document.entrySet().stream()
                        .collect(Collectors.toMap(Entry::getKey, Entry::getValue)));
    }

    public void setDelete() {
        isDelete = true;
    }

    public boolean isDelete() {
        return isDelete;
    }
}
