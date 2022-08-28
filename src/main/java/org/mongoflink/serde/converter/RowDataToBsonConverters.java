package org.mongoflink.serde.converter;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;

import org.bson.*;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.ZoneOffset;
import java.util.Arrays;

public class RowDataToBsonConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    public RowDataToBsonConverters() {}

    public RowDataToBsonConverter createConverter(LogicalType type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private RowDataToBsonConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (reuse, value) -> new BsonNull();
            case BOOLEAN:
                return (reuse, value) -> new BsonBoolean((Boolean) value);
            case TINYINT:
                return (reuse, value) -> new BsonInt32((Byte) value);
            case SMALLINT:
                return (reuse, value) -> new BsonInt32((Short) value);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (reuse, value) -> new BsonInt32((Integer) value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (reuse, value) -> new BsonInt64((Long) value);
            case FLOAT:
                return (reuse, value) -> new BsonDouble((Float) value);
            case DOUBLE:
                return (reuse, value) -> new BsonDouble((Double) value);
            case CHAR:
            case VARCHAR:
                return (reuse, value) -> new BsonString(value.toString());
            case BINARY:
            case VARBINARY:
                return (reuse, value) -> new BsonBinary((byte[]) value);
            case DATE:
                return this.createDateConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this.createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this.createTimestampWithLocalZone();
            case DECIMAL:
                return this.createDecimalConverter();
            case ARRAY:
                return this.createArrayConverter((ArrayType) type);
            case MAP:
                MapType mapType = (MapType) type;
                return this.createMapConverter(
                        mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return this.createMapConverter(
                        multisetType.asSummaryString(),
                        multisetType.getElementType(),
                        new IntType());
            case ROW:
                return this.createRowConverter((RowType) type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToBsonConverter createDecimalConverter() {
        return (reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return new BsonDecimal128(new Decimal128(bd));
        };
    }

    private RowDataToBsonConverter createDateConverter() {
        return (reuse, value) -> {
            int days = (Integer) value;
            int epochMillis = days * 24 * 60 * 60 * 1000;
            return new BsonInt64(epochMillis);
        };
    }

    private RowDataToBsonConverter createTimestampConverter() {
        return (reuse, value) -> {
            TimestampData timestamp = (TimestampData) value;
            return new BsonDateTime(timestamp.getMillisecond());
        };
    }

    private RowDataToBsonConverter createTimestampWithLocalZone() {
        return (reuse, value) -> {
            TimestampData timestampWithLocalZone = (TimestampData) value;
            return new BsonDateTime(
                    timestampWithLocalZone.toInstant().atOffset(ZoneOffset.UTC).toEpochSecond()
                            * 1_000L);
        };
    }

    private RowDataToBsonConverter createArrayConverter(ArrayType type) {
        LogicalType elementType = type.getElementType();
        RowDataToBsonConverter elementConverter = this.createConverter(elementType);
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return (reuse, value) -> {
            BsonArray node;
            if (reuse != null && !reuse.isNull()) {
                node = (BsonArray) reuse;
                node.clear();
            } else {
                node = new BsonArray();
            }

            ArrayData array = (ArrayData) value;
            int numElements = array.size();

            for (int i = 0; i < numElements; ++i) {
                Object element = elementGetter.getElementOrNull(array, i);
                node.add(elementConverter.convert(null, element));
            }

            return node;
        };
    }

    private RowDataToBsonConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (keyType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "JSON format doesn't support non-string as key type of map. The type is: "
                            + typeSummary);
        } else {
            RowDataToBsonConverter valueConverter = this.createConverter(valueType);
            ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
            return (reuse, object) -> {
                BsonDocument node;
                if (reuse != null && !reuse.isNull()) {
                    node = (BsonDocument) reuse;
                    node.clear();
                } else {
                    node = new BsonDocument();
                }

                MapData map = (MapData) object;
                ArrayData keyArray = map.keyArray();
                ArrayData valueArray = map.valueArray();
                int numElements = map.size();

                for (int i = 0; i < numElements; ++i) {
                    String fieldName;
                    if (keyArray.isNullAt(i)) {
                        // ignore null keys in the map
                        continue;
                    } else {
                        fieldName = keyArray.getString(i).toString();
                    }

                    Object value = valueGetter.getElementOrNull(valueArray, i);
                    node.put(fieldName, valueConverter.convert(node.get(fieldName), value));
                }

                return node;
            };
        }
    }

    private RowDataToBsonConverter createRowConverter(RowType type) {
        String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        RowDataToBsonConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(RowDataToBsonConverter[]::new);
        int fieldCount = type.getFieldCount();
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];

        for (int i = 0; i < fieldCount; ++i) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return (reuse, value) -> {
            BsonDocument node;
            if (reuse != null && !reuse.isNull()) {
                node = (BsonDocument) reuse;
            } else {
                node = new BsonDocument();
            }

            RowData row = (RowData) value;

            for (int i = 0; i < fieldCount; ++i) {
                String fieldName = fieldNames[i];
                try {
                    Object field = fieldGetters[i].getFieldOrNull(row);
                    node.put(fieldName, fieldConverters[i].convert(null, field));
                } catch (Throwable var12) {
                    throw new RuntimeException(
                            String.format("Fail to serialize at field: %s.", fieldName), var12);
                }
            }

            return node;
        };
    }

    private RowDataToBsonConverter wrapIntoNullableConverter(RowDataToBsonConverter converter) {
        return (reuse, object) ->
                object == null ? new BsonNull() : converter.convert(reuse, object);
    }

    public interface RowDataToBsonConverter extends Serializable {
        BsonValue convert(BsonValue reusedContainer, Object value);
    }
}
