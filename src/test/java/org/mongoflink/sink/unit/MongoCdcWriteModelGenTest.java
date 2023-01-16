package org.mongoflink.sink.unit;

import org.mongoflink.bson.CdcDocument;
import org.mongoflink.sink.CommittableCdcTransaction;

import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;

import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class MongoCdcWriteModelGenTest {
    /* unit test to ensure that the correct WriteModel's are generated for a given set of CdcDocuments to be sent via a Mongo transaction
     */

    private static final String json =
            "{\n"
                    + "\"name\": \"A Brief History of Time\",\n"
                    + "\"author\": \"Stephen Hawking\",\n"
                    + "\"language\": \"English\",\n"
                    + "\"publication year\": 1988\n"
                    + "}";

    private static final Document mockDocument = Document.parse(json);

    private CdcDocument getDeleteDoc() {
        CdcDocument doc = new CdcDocument(mockDocument);
        doc.setDelete();
        return doc;
    }

    private List<Document> getDocuments() {
        return List.of(
                new CdcDocument(mockDocument),
                new CdcDocument(mockDocument),
                getDeleteDoc(),
                new CdcDocument(mockDocument),
                getDeleteDoc(),
                getDeleteDoc(),
                new CdcDocument(mockDocument),
                getDeleteDoc());
    }

    @Test
    public void testWriteModelGen() {
        List<Document> testDocs = getDocuments();

        List<WriteModel<Document>> writes =
                CommittableCdcTransaction.getWrites(
                        getDocuments(),
                        List.of("name").toArray(new String[0]),
                        new UpdateOptions());

        Assert.assertEquals(testDocs.size(), writes.size());

        for (int pos = 0; pos < writes.size(); pos++) {
            if (((CdcDocument) testDocs.get(pos)).isDelete()) {
                Assert.assertThat(writes.get(pos), instanceOf(DeleteOneModel.class));
            } else {
                Assert.assertThat(writes.get(pos), instanceOf(UpdateOneModel.class));
            }
        }
    }
}
