package org.mongoflink.serde.table;

import org.mongoflink.serde.DocumentDeserializer;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;

import org.bson.Document;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;

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

    public DocumentRowDataDeserializer(String[] fieldNames, DataType[] projectedFieldType) {
        if (fieldNames == null || fieldNames.length < 1) {
            throw new IllegalArgumentException("fieldName is empty");
        }
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
            switch (fieldType.getTypeRoot()) {
                case NULL:
                    rowData.setField(i, null);
                    break;
                case BOOLEAN:
                case FLOAT:
                case DOUBLE:
                case INTERVAL_YEAR_MONTH:
                case INTERVAL_DAY_TIME:
                    rowData.setField(i, o);
                    break;
                case TINYINT:
                    rowData.setField(i, ((Integer) o).byteValue());
                    break;
                case SMALLINT:
                    rowData.setField(i, o instanceof Integer ? ((Integer) o).shortValue() : o);
                    break;
                case INTEGER:
                    rowData.setField(i, o);
                    break;
                case BIGINT:
                    rowData.setField(i, o);
                    break;
                case DECIMAL:
                    final int precision = ((DecimalType) fieldType).getPrecision();
                    final int scale = ((DecimalType) fieldType).getScale();
                    // using decimal(20, 0) to support db type bigint unsigned, user should define
                    // decimal(20, 0) in SQL,
                    // but other precision like decimal(30, 0) can work too from lenient
                    // consideration.
                    rowData.setField(
                            i,
                            o instanceof BigInteger
                                    ? DecimalData.fromBigDecimal(
                                            new BigDecimal((BigInteger) o, 0), precision, scale)
                                    : DecimalData.fromBigDecimal((BigDecimal) o, precision, scale));
                    break;
                case DATE:
                    ZonedDateTime date = ((Date) o).toInstant().atZone(ZoneId.systemDefault());
                    rowData.setField(i, date.toLocalDate().toEpochDay());
                    break;
                case TIMESTAMP_WITH_TIME_ZONE:
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    rowData.setField(i, TimestampData.fromEpochMillis(((Date) o).getTime()));
                    break;
                case CHAR:
                case VARCHAR:
                    rowData.setField(i, StringData.fromString((String) o));
                    break;
                case BINARY:
                case VARBINARY:
                    rowData.setField(i, o);
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                case ARRAY:
                case ROW:
                case MAP:
                case MULTISET:
                case RAW:
                default:
                    throw new UnsupportedOperationException("Unsupported type:" + fieldType);
            }
        }
        return rowData;
    }
}
