package org.mongoflink.serde.converter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;

import java.io.Serializable;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

public class RowDataToJsonConverters implements Serializable{

    private static final long serialVersionUID = 1L;
    private final TimestampFormat timestampFormat;
    private final JsonFormatOptions.MapNullKeyMode mapNullKeyMode;
    private final String mapNullKeyLiteral;

    public RowDataToJsonConverters(
            TimestampFormat timestampFormat,
            JsonFormatOptions.MapNullKeyMode mapNullKeyMode,
            String mapNullKeyLiteral) {
        this.timestampFormat = timestampFormat;
        this.mapNullKeyMode = mapNullKeyMode;
        this.mapNullKeyLiteral = mapNullKeyLiteral;
    }

    public RowDataToJsonConverter createConverter(LogicalType type) {
        return this.wrapIntoNullableConverter(this.createNotNullConverter(type));
    }

    private RowDataToJsonConverter createNotNullConverter(LogicalType type) {
        switch(type.getTypeRoot()) {
            case NULL:
                return (mapper, reuse, value) -> mapper.getNodeFactory().nullNode();
            case BOOLEAN:
                return (mapper, reuse, value) -> mapper.getNodeFactory().booleanNode((Boolean)value);
            case TINYINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Byte)value);
            case SMALLINT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Short)value);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Integer)value);
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Long)value);
            case FLOAT:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Float)value);
            case DOUBLE:
                return (mapper, reuse, value) -> mapper.getNodeFactory().numberNode((Double)value);
            case CHAR:
            case VARCHAR:
                return (mapper, reuse, value) -> mapper.getNodeFactory().textNode(value.toString());
            case BINARY:
            case VARBINARY:
                return (mapper, reuse, value) -> mapper.getNodeFactory().binaryNode((byte[])value);
            case DATE:
                return this.createDateConverter();
            case TIME_WITHOUT_TIME_ZONE:
                return this.createTimeConverter();
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                return this.createTimestampConverter();
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return this.createTimestampWithLocalZone();
            case DECIMAL:
                return this.createDecimalConverter();
            case ARRAY:
                return this.createArrayConverter((ArrayType)type);
            case MAP:
                MapType mapType = (MapType)type;
                return this.createMapConverter(mapType.asSummaryString(), mapType.getKeyType(), mapType.getValueType());
            case MULTISET:
                MultisetType multisetType = (MultisetType)type;
                return this.createMapConverter(multisetType.asSummaryString(), multisetType.getElementType(), new IntType());
            case ROW:
                return this.createRowConverter((RowType)type);
            case RAW:
            default:
                throw new UnsupportedOperationException("Not support to parse type: " + type);
        }
    }

    private RowDataToJsonConverter createDecimalConverter() {
        return (mapper, reuse, value) -> {
            BigDecimal bd = ((DecimalData)value).toBigDecimal();
            return mapper.getNodeFactory().numberNode(bd);
        };
    }

    private RowDataToJsonConverter createDateConverter() {
        return (mapper, reuse, value) -> {
            int days = (Integer)value;
            LocalDate date = LocalDate.ofEpochDay(days);
            return mapper.getNodeFactory().textNode(DateTimeFormatter.ISO_LOCAL_DATE.format(date));
        };
    }

    private RowDataToJsonConverter createTimeConverter() {
        return (mapper, reuse, value) -> {
            int millisecond = (Integer)value;
            LocalTime time = LocalTime.ofSecondOfDay((long)millisecond / 1000L);
            return mapper.getNodeFactory().textNode(TimeFormats.SQL_TIME_FORMAT.format(time));
        };
    }

    private RowDataToJsonConverter createTimestampConverter() {
        switch(this.timestampFormat) {
            case ISO_8601:
                return (mapper, reuse, value) -> {
                    TimestampData timestamp = (TimestampData)value;
                    return mapper.getNodeFactory().textNode(TimeFormats.ISO8601_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            case SQL:
                return (mapper, reuse, value) -> {
                    TimestampData timestamp = (TimestampData)value;
                    return mapper.getNodeFactory().textNode(TimeFormats.SQL_TIMESTAMP_FORMAT.format(timestamp.toLocalDateTime()));
                };
            default:
                throw new TableException("Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToJsonConverter createTimestampWithLocalZone() {
        switch(this.timestampFormat) {
            case ISO_8601:
                return (mapper, reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData)value;
                    return mapper.getNodeFactory().textNode(TimeFormats.ISO8601_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(timestampWithLocalZone.toInstant().atOffset(ZoneOffset.UTC)));
                };
            case SQL:
                return (mapper, reuse, value) -> {
                    TimestampData timestampWithLocalZone = (TimestampData)value;
                    return mapper.getNodeFactory().textNode(TimeFormats.SQL_TIMESTAMP_WITH_LOCAL_TIMEZONE_FORMAT.format(timestampWithLocalZone.toInstant().atOffset(ZoneOffset.UTC)));
                };
            default:
                throw new TableException("Unsupported timestamp format. Validator should have checked that.");
        }
    }

    private RowDataToJsonConverter createArrayConverter(ArrayType type) {
        LogicalType elementType = type.getElementType();
        RowDataToJsonConverter elementConverter = this.createConverter(elementType);
        ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(elementType);
        return (mapper, reuse, value) -> {
            ArrayNode node;
            if (reuse != null && !reuse.isNull()) {
                node = (ArrayNode)reuse;
                node.removeAll();
            } else {
                node = mapper.createArrayNode();
            }

            ArrayData array = (ArrayData)value;
            int numElements = array.size();

            for(int i = 0; i < numElements; ++i) {
                Object element = elementGetter.getElementOrNull(array, i);
                node.add(elementConverter.convert(mapper, null, element));
            }

            return node;
        };
    }

    private RowDataToJsonConverter createMapConverter(String typeSummary, LogicalType keyType, LogicalType valueType) {
        if (keyType.getTypeRoot().getFamilies().contains(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new UnsupportedOperationException("JSON format doesn't support non-string as key type of map. The type is: " + typeSummary);
        } else {
            RowDataToJsonConverter valueConverter = this.createConverter(valueType);
            ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(valueType);
            return (mapper, reuse, object) -> {
                ObjectNode node;
                if (reuse != null && !reuse.isNull()) {
                    node = (ObjectNode)reuse;
                    node.removeAll();
                } else {
                    node = mapper.createObjectNode();
                }

                MapData map = (MapData)object;
                ArrayData keyArray = map.keyArray();
                ArrayData valueArray = map.valueArray();
                int numElements = map.size();

                for(int i = 0; i < numElements; ++i) {
                    String fieldName;
                    if (keyArray.isNullAt(i)) {
                        switch(this.mapNullKeyMode) {
                            case LITERAL:
                                fieldName = this.mapNullKeyLiteral;
                                break;
                            case DROP:
                                continue;
                            case FAIL:
                                throw new RuntimeException(String.format("JSON format doesn't support to serialize map data with null keys. You can drop null key entries or encode null in literals by specifying %s option.", JsonFormatOptions.MAP_NULL_KEY_MODE.key()));
                            default:
                                throw new RuntimeException("Unsupported map null key mode. Validator should have checked that.");
                        }
                    } else {
                        fieldName = keyArray.getString(i).toString();
                    }

                    Object value = valueGetter.getElementOrNull(valueArray, i);
                    node.set(fieldName, valueConverter.convert(mapper, node.get(fieldName), value));
                }

                return node;
            };
        }
    }

    private RowDataToJsonConverter createRowConverter(RowType type) {
        String[] fieldNames = type.getFieldNames().toArray(new String[0]);
        LogicalType[] fieldTypes = type.getFields().stream().map(RowType.RowField::getType).toArray(LogicalType[]::new);
        RowDataToJsonConverter[] fieldConverters =
                Arrays.stream(fieldTypes).map(this::createConverter).toArray(RowDataToJsonConverter[]::new);
        int fieldCount = type.getFieldCount();
        RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldTypes.length];

        for(int i = 0; i < fieldCount; ++i) {
            fieldGetters[i] = RowData.createFieldGetter(fieldTypes[i], i);
        }

        return (mapper, reuse, value) -> {
            ObjectNode node;
            if (reuse != null && !reuse.isNull()) {
                node = (ObjectNode)reuse;
            } else {
                node = mapper.createObjectNode();
            }

            RowData row = (RowData)value;

            for(int i = 0; i < fieldCount; ++i) {
                String fieldName = fieldNames[i];

                try {
                    Object field = fieldGetters[i].getFieldOrNull(row);
                    node.set(fieldName, fieldConverters[i].convert(mapper, node.get(fieldName), field));
                } catch (Throwable var12) {
                    throw new RuntimeException(String.format("Fail to serialize at field: %s.", fieldName), var12);
                }
            }

            return node;
        };
    }

    private RowDataToJsonConverter wrapIntoNullableConverter(RowDataToJsonConverter converter) {
        return (mapper, reuse, object) -> object == null ?
                mapper.getNodeFactory().nullNode() : converter.convert(mapper, reuse, object);
    }

    public interface RowDataToJsonConverter extends Serializable {
        JsonNode convert(ObjectMapper var1, JsonNode var2, Object var3);
    }

}
