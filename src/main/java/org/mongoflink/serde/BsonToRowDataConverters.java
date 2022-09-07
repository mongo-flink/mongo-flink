package org.mongoflink.serde;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;
import org.apache.flink.types.RowKind;

import org.bson.*;
import org.bson.types.Binary;
import org.bson.types.Decimal128;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

public class BsonToRowDataConverters implements Serializable {

    private static final long serialVersionUID = 1L;

    public BsonToRowDataConverters() {}

    public BsonToRowDataConverter createConverter(LogicalType type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private BsonToRowDataConverter createNotNullConverter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (reuse, value) -> null;
            case BOOLEAN:
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
            case DOUBLE:
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (reuse, value) -> value;
            case TINYINT:
                return (reuse, value) -> ((Integer) value).byteValue();
            case SMALLINT:
                return (reuse, value) -> ((Integer) value).shortValue();
            case FLOAT:
                return (reuse, value) -> ((Double) value).floatValue();
            case CHAR:
            case VARCHAR:
                return (reuse, value) -> StringData.fromString((String) value);
            case BINARY:
            case VARBINARY:
                return (reuse, value) -> ((Binary) value).getData();
            case DATE:
                return this.createDateConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this.createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this.createTimestampWithLocalZone();
            case DECIMAL:
                return (reuse, value) -> {
                    final int precision = ((DecimalType) type).getPrecision();
                    final int scale = ((DecimalType) type).getScale();
                    return DecimalData.fromBigDecimal(
                            ((BsonDecimal128) value).getValue().bigDecimalValue(),
                            precision,
                            scale);
                };
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

    private BsonToRowDataConverter createDecimalConverter() {
        return (reuse, value) -> {
            BigDecimal bd = ((DecimalData) value).toBigDecimal();
            return new BsonDecimal128(new Decimal128(bd));
        };
    }

    private BsonToRowDataConverter createDateConverter() {
        return (reuse, value) -> {
            ZonedDateTime date = Instant.ofEpochMilli((Long) value).atZone(ZoneId.systemDefault());
            return date.toLocalDate().toEpochDay();
        };
    }

    private BsonToRowDataConverter createTimestampConverter() {
        return (reuse, value) -> TimestampData.fromEpochMillis((Long) value);
    }

    private BsonToRowDataConverter createTimestampWithLocalZone() {
        return (reuse, value) -> TimestampData.fromEpochMillis((Long) value);
    }

    private BsonToRowDataConverter createArrayConverter(ArrayType type) {
        LogicalType elementType = type.getElementType();
        BsonToRowDataConverter elementConverter = this.createConverter(elementType);
        return (reuse, value) ->
                new GenericArrayData(
                        ((ArrayList) value)
                                .stream().map(e -> elementConverter.convert(null, e)).toArray());
    }

    private BsonToRowDataConverter createMapConverter(
            String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (!keyType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException(
                    "Bson format doesn't support non-string as key type of map. The type is: "
                            + typeSummary);
        } else {
            BsonToRowDataConverter valueConverter = this.createConverter(valueType);
            return (reuse, value) -> {
                Map<StringData, Object> map = new HashMap<>();
                Document document = (Document) value;
                for (String key : document.keySet()) {
                    map.put(
                            StringData.fromString(key),
                            valueConverter.convert(null, document.get(key)));
                }
                return new GenericMapData(map);
            };
        }
    }

    private BsonToRowDataConverter createRowConverter(RowType type) {
        String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        LogicalType[] fieldTypes =
                type.getFields().stream()
                        .map(RowType.RowField::getType)
                        .toArray(LogicalType[]::new);
        BsonToRowDataConverter[] fieldConverters =
                Arrays.stream(fieldTypes)
                        .map(this::createConverter)
                        .toArray(BsonToRowDataConverter[]::new);
        int fieldCount = type.getFieldCount();

        return (reuse, value) -> {
            GenericRowData containerRow;
            if (reuse != null) {
                containerRow = (GenericRowData) reuse;
            } else {
                containerRow = new GenericRowData(RowKind.INSERT, fieldCount);
            }

            Document document = (Document) value;
            for (int i = 0; i < fieldCount; ++i) {
                String fieldName = fieldNames[i];
                Object fieldValue = document.get(fieldName);
                containerRow.setField(i, fieldConverters[i].convert(null, fieldValue));
            }
            return containerRow;
        };
    }

    private BsonToRowDataConverter wrapIntoNullableConverter(BsonToRowDataConverter converter) {
        return (reuse, object) -> object == null ? null : converter.convert(reuse, object);
    }

    public interface BsonToRowDataConverter extends Serializable {
        Object convert(Object reusedContainer, Object value);
    }
}
