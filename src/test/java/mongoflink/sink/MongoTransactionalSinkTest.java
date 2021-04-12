package mongoflink.sink;


import mongoflink.config.MongoOptions;
import mongoflink.serde.DocumentSerializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.bson.Document;

import org.junit.Test;

import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.*;

public class MongoTransactionalSinkTest extends MongoSinkTestBase {

    @Test
    public void testWrite() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointInterval(1000L);

        // if these rows are not multiple times of rps, there would be the records remaining not flushed
        // after the last checkpoint
        long rps = 50;
        long rows = 1000L;

        Properties properties = new Properties();
        properties.setProperty(MongoOptions.TRANSACTION_ENABLED, "true");
        properties.setProperty(MongoOptions.BULK_FLUSH_ON_CHECKPOINT, "true");
        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
                .returns(String.class)
                .sinkTo(new MongoSink<>(CONNECT_STRING, DATABASE_NAME, COLLECTION,
                        new StringDocumentSerializer(), properties));
        StreamGraph streamGraph = env.getStreamGraph(MongoTransactionalSinkTest.class.getName());

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(streamGraph.getJobGraph());
        }

        assertEquals(rows, mongo.getDatabase(DATABASE_NAME).getCollection(COLLECTION).countDocuments());
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