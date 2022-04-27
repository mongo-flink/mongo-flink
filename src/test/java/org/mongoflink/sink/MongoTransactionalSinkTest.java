package org.mongoflink.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.mongoflink.config.MongoConnectorOptions;

import org.junit.Test;

import java.time.Duration;
import java.time.temporal.ChronoUnit;

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

        MongoConnectorOptions options = MongoConnectorOptions.builder()
                .withConnectString(CONNECT_STRING)
                .withDatabase(DATABASE_NAME)
                .withCollection(COLLECTION)
                .withTransactionEnable(false)
                .withFlushOnCheckpoint(false)
                .withFlushSize(1_000)
                .withFlushInterval(Duration.of(10_000L, ChronoUnit.MILLIS))
                .build();
        env.addSource(new DataGeneratorSource<>(new StringGenerator(), rps, rows))
                .returns(String.class)
                .sinkTo(new MongoSink<>(new StringDocumentSerializer(), options));
        StreamGraph streamGraph = env.getStreamGraph();

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