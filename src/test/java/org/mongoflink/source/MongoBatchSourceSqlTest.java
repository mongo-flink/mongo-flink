package org.mongoflink.source;

import com.google.common.collect.Lists;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.CollectionUtil;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;
import org.mongoflink.EmbeddedMongoTestBase;
import org.mongoflink.internal.connection.MongoClientProvider;
import org.mongoflink.internal.connection.MongoColloctionProviders;

import java.util.List;
import java.util.stream.Collectors;

public class MongoBatchSourceSqlTest extends EmbeddedMongoTestBase {


    protected static String DATABASE_NAME = "source_test";
    protected static String COLLECTION = "batch_read";

    @Test
    public void testRead() throws Exception {
        MongoClientProvider clientProvider = MongoColloctionProviders.getBuilder()
                .connectionString(CONNECT_STRING)
                .database(DATABASE_NAME)
                .collection(COLLECTION).build();

        // generate documents
        List<Document> docs = Lists.newArrayList();

        for (int i = 0; i < 10_000; i++) {
            // roughly 56b per document
            docs.add(
                    new Document()
                            .append("user_id", i)
                            .append("gold", i)
                            .append("level", i)
                            .append("grade", "grade" + i)
                            .append("class", "class" + i)
                            .append("score", (i) / 1.5)
                            .append("sex", i % 2 == 0)
                            .append("hobby", i % 10 == 0 ? "football" : null)
            );
        }
        clientProvider.getDefaultCollection().insertMany(docs);
        TableEnvironment env = TableEnvironment.create(EnvironmentSettings.newInstance().inBatchMode().build());
        env.executeSql("create table mongo_test (" +
                "    user_id int," +
                "    gold int," +
                "    level int," +
                "    grade string," +
                "    class string," +
                "    score double," +
                "    sex boolean," +
                "    hobby string" +
                ") with (" +
                "    'connector'='mongo'," +
                "    'connect_string' = 'mongodb://" + HOST + ":" + PORT + "/" + DATABASE_NAME + "'," +
                "    'database' = '" + DATABASE_NAME + "'," +
                "    'collection' = '" + COLLECTION + "'" +
                ")"
        );

        TableResult tableResult = env.executeSql(
                "select user_id, gold, level from mongo_test " +
                        "where user_id > 999 " +
                        "and score > 100 " +
                        "and (sex = 'false' or (hobby is not null or (level > 10 and gold > 10))) " +
                        "order by user_id desc limit 10");
        tableResult.print();
        CloseableIterator<Row> collected = env.executeSql(
                "select user_id, gold, level from mongo_test " +
                        "where user_id > 999 " +
                        "and score > 100 " +
                        "and (sex = 'false' or (hobby is not null or (level > 10 and gold > 10))) " +
                        "order by user_id desc limit 10").collect();

        List<String> result =
                CollectionUtil.iteratorToList(collected).stream()
                        .map(Row::toString)
                        .sorted()
                        .collect(Collectors.toList());
        Assert.assertEquals("query result size not size", 10, result.size());
        Assert.assertEquals("query result not same.", "[+I[9990, 9990, 9990], +I[9991, 9991, 9991], +I[9992, 9992, 9992], +I[9993, 9993, 9993], +I[9994, 9994, 9994], +I[9995, 9995, 9995], +I[9996, 9996, 9996], +I[9997, 9997, 9997], +I[9998, 9998, 9998], +I[9999, 9999, 9999]]", result.toString());
    }


}
