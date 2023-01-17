package org.mongoflink.serde;

import org.mongoflink.bson.CdcDocument;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.codecs.CdcDocumentCodec;
import org.bson.codecs.DecoderContext;

/** convert rowdata to document. */
public class RowDataDocumentSerializer implements DocumentSerializer<RowData> {

    private final RowDataToBsonConverters.RowDataToBsonConverter bsonConverter;

    private transient BsonDocument node;

    public RowDataDocumentSerializer(LogicalType logicalType) {
        this.bsonConverter = new RowDataToBsonConverters().createConverter(logicalType);
    }

    @Override
    public CdcDocument serialize(RowData row) {
        if (node == null) {
            node = new BsonDocument();
        }
        try {
            bsonConverter.convert(node, row);
            CdcDocumentCodec codec = new CdcDocumentCodec();
            DecoderContext decoderContext = DecoderContext.builder().build();
            CdcDocument doc =
                    (CdcDocument) codec.decode(new BsonDocumentReader(node), decoderContext);
            if (row.getRowKind().equals(RowKind.DELETE)) {
                doc.setDelete();
            }
            return doc;
        } catch (Exception e) {
            throw new RuntimeException("can not serialize row '" + row + "'. ", e);
        }
    }
}
