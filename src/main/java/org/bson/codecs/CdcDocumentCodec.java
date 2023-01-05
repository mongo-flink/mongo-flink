package org.bson.codecs;

import org.mongoflink.bson.CdcDocument;

import org.bson.*;

/**
 * CdcDocumentCodec is a DocumentCodec with an overriden decode method that serializes bson to a
 * CdcDocument.
 */
public class CdcDocumentCodec extends DocumentCodec {

    public CdcDocumentCodec() {
        super();
    }

    @Override
    public Document decode(final BsonReader reader, final DecoderContext decoderContext) {
        Document doc = super.decode(reader, decoderContext);
        return new CdcDocument(doc);
    }
}
