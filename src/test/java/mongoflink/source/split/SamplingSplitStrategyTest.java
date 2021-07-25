package mongoflink.source.split;

import com.google.common.collect.Lists;
import mongoflink.EmbeddedMongoTestBase;
import mongoflink.internal.connection.MongoClientProvider;
import mongoflink.internal.connection.MongoColloctionProviders;
import mongoflink.source.split.MongoSplit;
import mongoflink.source.split.MongoSplitStrategy;
import mongoflink.source.split.SamplingSplitStrategy;
import org.bson.Document;
import org.junit.Test;

import java.util.List;
import java.util.Random;

import static com.mongodb.client.model.Filters.*;
import static org.junit.Assert.*;

public class SamplingSplitStrategyTest extends EmbeddedMongoTestBase {

    protected static String DATABASE_NAME = "sampling_split_test";
    protected static String COLLECTION = "sampling_split_test";

    @Test
    public void testDefaultSplit() {

        MongoClientProvider clientProvider =  MongoColloctionProviders.getBuilder()
                .connectionString(CONNECT_STRING)
                .database(DATABASE_NAME)
                .collection(COLLECTION).build();

        // generate documents
        List<Document> docs = Lists.newArrayList();
        Random random = new Random();
        for (int i=0;i<10_000;i++) {
            // roughly 56b per document
            docs.add(
                    new Document()
                            .append("user_id", i + 1)
                            .append("gold", random.nextInt() % 100)
                            .append("level", random.nextInt() % 255)
            );
        }

        clientProvider.getDefaultCollection().insertMany(docs);

        MongoSplitStrategy strategyByObjectId = SamplingSplitStrategy.builder()
                .setClientProvider(clientProvider)
                // set 10kb per split to generate more splits
                .setSizePerSplit(10240)
                .build();

        List<MongoSplit> splitsByObjectId = strategyByObjectId.split();

        assertTrue(splitsByObjectId.size() > 50);

        MongoSplitStrategy strategyByUserId = SamplingSplitStrategy.builder()
                .setClientProvider(clientProvider)
                .setSplitKey("user_id")
                .setMatchQuery(gte("user_id", 1000).toBsonDocument())
                .setSizePerSplit(10240)
                .build();

        List<MongoSplit> splitsByUserId = strategyByUserId.split();

        assertTrue(splitsByUserId.size() < splitsByObjectId.size());
    }
}
