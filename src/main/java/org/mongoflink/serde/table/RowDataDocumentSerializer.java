package org.mongoflink.serde.table;

import org.mongoflink.serde.DocumentSerializer;
import org.mongoflink.serde.converter.RowDataToBsonConverters;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;

/** convert rowdata to document. */
public class RowDataDocumentSerializer implements DocumentSerializer<RowData> {

    private final RowDataToBsonConverters.RowDataToBsonConverter bsonConverter;

    private transient BsonDocument node;

    public RowDataDocumentSerializer(LogicalType logicalType) {
        this.bsonConverter = new RowDataToBsonConverters().createConverter(logicalType);
    }

    @Override
    public Document serialize(RowData row) {
        if (node == null) {
            node = new BsonDocument();
        }
        try {
            bsonConverter.convert(node, row);
            DocumentCodec codec = new DocumentCodec();
            DecoderContext decoderContext = DecoderContext.builder().build();
            return codec.decode(new BsonDocumentReader(node), decoderContext);
        } catch (Exception e) {
            throw new RuntimeException("can not serialize row '" + row + "'. ", e);
        }
    }
}
