package mongoflink.sink;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.MongodConfig;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import mongoflink.serde.DocumentSerializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;

import java.util.Random;

/**
 * Base class for tests for MongoSink.
 **/
public class MongoSinkTestBase {

    protected static final String HOST = "127.0.0.1";
    protected static final int PORT = 27018;
    protected static final String DATABASE_NAME = "bulkwrite";
    protected static final String COLLECTION = "transactional-4_0";
    protected static final String CONNECT_STRING = String.format("mongodb://%s:%d/%s", HOST, PORT, DATABASE_NAME);


    protected MongodExecutable mongodExe;
    protected MongodProcess mongod;
    protected MongoClient mongo;

    @Before
    public void before() throws Exception {
        MongodStarter starter = MongodStarter.getDefaultInstance();
        MongodConfig mongodConfig = MongodConfig.builder()
                .version(Version.Main.V4_0)
                .net(new Net(HOST, PORT, Network.localhostIsIPv6()))
                .build();
        this.mongodExe = starter.prepare(mongodConfig);
        this.mongod = mongodExe.start();
        this.mongo = MongoClients.create(CONNECT_STRING);
    }


    @After
    public void after() throws Exception {
        if (this.mongo != null) {
            mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).drop();
            mongo.close();
        }
        if (this.mongod != null) {
            this.mongod.stop();
            this.mongodExe.stop();
        }
    }
}


class StringDocumentSerializer implements DocumentSerializer<String> {

    @Override
    public Document serialize(String string) {
        Document document = new Document();
        String[] elements = string.split(",");
        document.append("word", elements[0]);
        document.append("count", Integer.parseInt(elements[1]));
        return document;
    }
}

class StringGenerator implements DataGenerator<String> {

    private int count;

    @Override
    public void open(String s, FunctionInitializationContext functionInitializationContext, RuntimeContext runtimeContext) throws Exception {
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public String next() {
        return "key" + count++ + "," + new Random().nextInt();
    }
}
