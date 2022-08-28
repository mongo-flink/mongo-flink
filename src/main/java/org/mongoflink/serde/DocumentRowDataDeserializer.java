package org.mongoflink.serde;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

import org.bson.*;

/**
 * assemble RowData by document key (which is field name.), because mongo result columns in a
 * different order than projection eg: db.collection.find({"a": {$gt: 1}}, {"a": 1, "b": 1, "c": 1,
 * "d": 1, "_id": 0}) the order of result column may be. +-----+-----+-----+------+ |b |c |d |a |
 * +-----+-----+-----+------+ |11001|11001|7334 |1001 | +-----+-----+-----+------+
 *
 * <p>So we cannot construct RowData using indexes. we should use key(fieldName)
 */
public class DocumentRowDataDeserializer implements DocumentDeserializer<RowData> {

    private final String[] fieldNames;

    private final DataType[] dataTypes;

    private final BsonToRowDataConverters bsonConverters;

    public DocumentRowDataDeserializer(String[] fieldNames, DataType[] projectedFieldType) {
        if (fieldNames == null || fieldNames.length < 1) {
            throw new IllegalArgumentException("fieldName is empty");
        }
        this.bsonConverters = new BsonToRowDataConverters();
        this.fieldNames = fieldNames;
        this.dataTypes = projectedFieldType;
    }

    /**
     * @see RowData class doc.
     * @param document The input {@link Document}.
     * @return flink RowData
     */
    @Override
    public RowData deserialize(Document document) {
        GenericRowData rowData = new GenericRowData(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            String fieldName = this.fieldNames[i];
            Object o = document.get(fieldName);
            DataType dataType = dataTypes[i];
            LogicalType fieldType = dataType.getLogicalType();
            rowData.setField(
                    i, bsonConverters.createConverter(dataType.getLogicalType()).convert(null, o));
        }
        return rowData;
    }
}
