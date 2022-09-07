package org.mongoflink.sink;

import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.internal.connection.MongoColloctionProviders;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCursor;

import com.google.common.collect.Lists;
import org.bson.Document;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.mongodb.client.model.Sorts.ascending;
import static org.junit.Assert.*;

public class MongoTableInsertSinkTest extends MongoSinkTestBase {

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

        // generate documents
        List<Document> sourceDocs = Lists.newArrayList();

        for (int i = 0; i < 10; i++) {
            // generate list field
            List<String> hobbies = new ArrayList<>(3);
            hobbies.add(HOBBIES.get(i % HOBBIES.size()));
            // generate object field
            Document tags = new Document();
            tags.put("k1", "v1");
            tags.put("k2", "v2");
            sourceDocs.add(
                    new Document()
                            .append("user_id", (long) i)
                            .append("gold", (double) i)
                            .append("level", i)
                            .append("grade", "grade" + (i))
                            .append("score", (i) / 1.5F)
                            .append("vip", (i) % 2 == 0)
                            .append("hobbies", hobbies)
                            .append("tags", tags));
        }
        clientProviderSource.getDefaultCollection().insertMany(sourceDocs);

        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.inBatchMode());
        env.executeSql(
                "create table mongo_test_source ("
                        + "    user_id bigint,"
                        + "    gold double,"
                        + "    level int,"
                        + "    grade string,"
                        + "    score float,"
                        + "    vip boolean,"
                        + "    hobbies array<string>,"
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

        env.executeSql(
                "create table mongo_test_sink ("
                        + "    user_id bigint,"
                        + "    gold double,"
                        + "    level int,"
                        + "    grade string,"
                        + "    score float,"
                        + "    vip boolean,"
                        + "    hobbies array<string>,"
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
                resultDoc.remove("_id");
                resultDocs.add(resultDoc);
            }
        }

        // compare in json format because float point numbers would not equal
        assertArrayEquals(
                sourceDocs.stream().peek(d -> d.remove("_id")).map(Document::toJson).toArray(),
                resultDocs.stream().map(Document::toJson).toArray());
    }

    private final List<String> HOBBIES =
            Lists.newArrayList("football", "baseball", "swimming", "hockey", null);
}
