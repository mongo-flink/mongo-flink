package org.mongoflink.serde.table;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.bson.Document;
import org.mongoflink.serde.DocumentDeserializer;

/**
 * assemble RowData by document key (which is field name.),
 * because mongo result columns in a different order than projection
 * eg:
 * db.collection.find({"a": {$gt: 1}}, {"a": 1, "b": 1, "c": 1, "d": 1, "_id": 0})
 * the order of result column may be.
 * +-----+-----+-----+------+
 * |b    |c    |d    |a     |
 * +-----+-----+-----+------+
 * |11001|11001|7334 |1001  |
 * +-----+-----+-----+------+
 * <p>
 * So we cannot construct RowData using indexes. we should use key(fieldName)
 */
public class DocumentRowDataDeserializer implements DocumentDeserializer<RowData> {

    private final String[] fieldNames;

    public DocumentRowDataDeserializer(String[] fieldNames) {
        if (fieldNames == null || fieldNames.length < 1) {
            throw new IllegalArgumentException("fieldName is empty");
        }
        this.fieldNames = fieldNames;
    }

    @Override
    public RowData deserialize(Document document) {
        GenericRowData rowData = new GenericRowData(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = this.fieldNames[i];
            Object o = document.get(fieldName);
            if (o instanceof String) {
                o = StringData.fromString((String) o);
            }

            rowData.setField(i, o);
        }
        return rowData;
    }

}

