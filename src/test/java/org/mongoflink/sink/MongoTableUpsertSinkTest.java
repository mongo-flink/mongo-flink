package org.mongoflink.sink;

import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.internal.connection.MongoColloctionProviders;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import com.google.common.collect.Lists;
import org.bson.BsonDocument;
import org.bson.BsonString;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.client.model.Sorts.ascending;
import static org.junit.Assert.assertEquals;

public class MongoTableUpsertSinkTest extends MongoSinkTestBase {

    protected static String DATABASE_NAME = "sink_test";
    protected static String SOURCE_COLLECTION = "batch_source";
    protected static String SINK_COLLECTION = "batch_sink";

    @Test
    public void testTableSink() throws ExecutionException, InterruptedException {

        MongoClientProvider clientProviderSource =
                MongoColloctionProviders.getBuilder()
                        .connectionString(CONNECT_STRING)
                        .database(DATABASE_NAME)
                        .collection(SOURCE_COLLECTION)
                        .build();

        MongoClientProvider clientProviderSink =
                MongoColloctionProviders.getBuilder()
                        .connectionString(CONNECT_STRING)
                        .database(DATABASE_NAME)
                        .collection(SINK_COLLECTION)
                        .build();

        // generate documents
        List<Document> sourceDocs = Lists.newArrayList();

        for (int i = 0; i < 10; i++) {
            // generate list field
            List<String> hobbies = new ArrayList<>(3);
            hobbies.add(HOBBIES.get(i % HOBBIES.size()));
            // generate object field
            BsonDocument tags = new BsonDocument();
            tags.put("k1", new BsonString("v1"));
            tags.put("k2", new BsonString("v2"));
            sourceDocs.add(
                    new Document()
                            .append("user_id", 10) // pin user_id for duplication
                            .append("gold", i + 10)
                            .append("level", i + 10)
                            .append("grade", "grade" + (i + 10))
                            .append("score", (i + 10) / 1.5)
                            .append("vip", (i + 10) % 2 == 0)
                            .append("hobby", hobbies)
                            .append("tags", tags));
        }
        clientProviderSource.getDefaultCollection().insertMany(sourceDocs);

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.executeSql(
                "create table mongo_test_source ("
                        + "    user_id int,"
                        + "    gold int,"
                        + "    level int,"
                        + "    grade string,"
                        + "    score double,"
                        + "    vip boolean,"
                        + "    hobby array<string>,"
                        + "    tags map<string, string>"
                        + ") with ("
                        + "    'connector'='mongo',"
                        + "    'connect_string' = '"
                        + CONNECT_STRING
                        + "',"
                        + "    'database' = '"
                        + DATABASE_NAME
                        + "',"
                        + "    'collection' = '"
                        + SOURCE_COLLECTION
                        + "'"
                        + ")");

        // primary key for upsert
        env.executeSql(
                "create table mongo_test_sink ("
                        + "    user_id int,"
                        + "    gold int,"
                        + "    level int,"
                        + "    grade string,"
                        + "    score double,"
                        + "    vip boolean,"
                        + "    hobby array<string>,"
                        + "    tags map<string, string>,"
                        + "    PRIMARY key(user_id) NOT ENFORCED"
                        + ") with ("
                        + "    'connector'='mongo',"
                        + "    'connect_string' = '"
                        + CONNECT_STRING
                        + "',"
                        + "    'database' = '"
                        + DATABASE_NAME
                        + "',"
                        + "    'collection' = '"
                        + SINK_COLLECTION
                        + "'"
                        + ")");

        TableResult tableResult =
                env.executeSql("insert into mongo_test_sink select * from mongo_test_source");
        tableResult.await();

        MongoClientProvider resultClientProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(CONNECT_STRING)
                        .database(DATABASE_NAME)
                        .collection(SINK_COLLECTION)
                        .build();

        FindIterable<Document> resultIterable =
                resultClientProvider.getDefaultCollection().find().sort(ascending("user_id"));
        List<Document> resultDocs = Lists.newArrayList();
        try (MongoCursor<Document> cursor = resultIterable.cursor()) {
            while (cursor.hasNext()) {
                Document resultDoc = cursor.next();
                resultDocs.add(resultDoc);
            }
        }

        assertEquals(1, resultDocs.size());
    }

    private final List<String> HOBBIES =
            Lists.newArrayList("football", "baseball", "swimming", "hockey", null);
}
