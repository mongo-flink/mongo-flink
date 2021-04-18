package mongoflink.config;

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
                PropertiesUtil.getLong(properties, MongoOptions.SINK_FLUSH_SIZE, 1000L));
        configuration.setBulkFlushInterval(
                PropertiesUtil.getLong(properties, MongoOptions.SINK_FLUSH_INTERVAL, 30_000L));
        return configuration;
    }
}
