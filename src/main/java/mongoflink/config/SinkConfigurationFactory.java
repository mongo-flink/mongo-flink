package mongoflink.config;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.PropertiesUtil;

import java.util.Properties;

/**
 * Simple factory for {@link SinkConfiguration}.
 **/
public class SinkConfigurationFactory {

    public static SinkConfiguration fromProperties(Properties properties) {
        SinkConfiguration configuration = new SinkConfiguration();
        configuration.setTransactional(
                PropertiesUtil.getBoolean(properties, MongoOptions.SINK_TRANSACTION_ENABLED, false));
        configuration.setFlushOnCheckpoint(
                PropertiesUtil.getBoolean(properties, MongoOptions.SINK_FLUSH_ON_CHECKPOINT, false));
        configuration.setBulkFlushSize(
                PropertiesUtil.getLong(properties, MongoOptions.SINK_FLUSH_SIZE, 1_000L));
        configuration.setBulkFlushInterval(
                PropertiesUtil.getLong(properties, MongoOptions.SINK_FLUSH_INTERVAL, 30_000L));

        // validate config
        if (configuration.isTransactional()) {
            Preconditions.checkArgument(configuration.isFlushOnCheckpoint(),
                    "`%s` must be true when the transactional sink is enabled",
                    MongoOptions.SINK_FLUSH_ON_CHECKPOINT);
        }
        Preconditions.checkArgument(configuration.getBulkFlushSize() > 0,
                "`%s` must be greater than 0",
                MongoOptions.SINK_FLUSH_SIZE);

        return configuration;
    }
}
