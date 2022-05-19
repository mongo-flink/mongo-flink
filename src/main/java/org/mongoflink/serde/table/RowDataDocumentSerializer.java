package org.mongoflink.serde.table;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.bson.Document;
import org.mongoflink.serde.DocumentSerializer;
import org.mongoflink.serde.converter.JsonFormatOptions;
import org.mongoflink.serde.converter.RowDataToJsonConverters;
import org.mongoflink.serde.converter.TimestampFormat;

/**
 * convert rowdata to document.
 */
public class RowDataDocumentSerializer implements DocumentSerializer<RowData> {

    private final RowDataToJsonConverters.RowDataToJsonConverter jsonConverter;

    private transient ObjectNode node;
    private final ObjectMapper mapper = new ObjectMapper();

    public RowDataDocumentSerializer(LogicalType logicalType) {
        this.jsonConverter = new RowDataToJsonConverters(TimestampFormat.SQL,
                JsonFormatOptions.MapNullKeyMode.LITERAL, null)
                .createConverter(logicalType);
    }

    @Override
    public Document serialize(RowData row) {
        if (node == null) {
            node = mapper.createObjectNode();
        }
        try {
            jsonConverter.convert(mapper, node, row);
            String s = mapper.writeValueAsString(node);
            return Document.parse(s);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("can not serialize row '" + row + "'. ", e);
        }
    }
}
