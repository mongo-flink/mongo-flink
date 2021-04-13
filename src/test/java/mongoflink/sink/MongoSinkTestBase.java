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
import org.junit.After;
import org.junit.Before;

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
