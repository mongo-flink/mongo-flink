package org.mongoflink.source;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.bson.Document;
import org.junit.Test;
import org.mongoflink.EmbeddedMongoTestBase;
import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.internal.connection.MongoColloctionProviders;
import org.mongoflink.serde.DocumentDeserializer;
import org.mongoflink.source.split.SamplingSplitStrategy;
import org.mongoflink.utils.ListSink;

import java.util.List;
import java.util.Random;

import static com.mongodb.client.model.Filters.gte;
import static org.junit.Assert.*;

/**
 * Tests for mongo source batch mode.
 **/
public class MongoBatchSourceTest extends EmbeddedMongoTestBase {

    protected static String DATABASE_NAME = "source_test";
    protected static String COLLECTION = "batch_read";

    @Test
    public void testRead() throws Exception {
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

        MongoSource<String> mongoSource = new MongoSource<>(
                clientProvider,
                (DocumentDeserializer<String>) Document::toJson,
                SamplingSplitStrategy.builder()
                        .setMatchQuery(gte("user_id", 1000).toBsonDocument())
                        .setClientProvider(clientProvider)
                        .build()
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000L);

        ListSink<String> sink = new ListSink<>();
        env.fromSource(mongoSource, WatermarkStrategy.noWatermarks(), "mongo_batch_source")
                .returns(String.class)
                .addSink(sink);

        JobExecutionResult result = env.execute("test_batch_read");
        result.getNetRuntime();

        // 1000-10000
        assertEquals( 9001, ListSink.getElementsSet().size());
    }

}
