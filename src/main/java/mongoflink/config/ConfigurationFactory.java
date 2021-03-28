package mongoflink.config;

import org.apache.flink.util.PropertiesUtil;

import java.util.Properties;

/**
 * Simple factory for {@link Configuration}.
 **/
public class ConfigurationFactory {

    public static Configuration fromProperties(Properties properties) {
        Configuration configuration = new Configuration();
        configuration.setFlushOnCheckpoint(
                PropertiesUtil.getBoolean(properties, MongoOptions.TRANSACTION_ENABLED, false));
        configuration.setFlushOnCheckpoint(
                PropertiesUtil.getBoolean(properties, MongoOptions.BULK_FLUSH_ON_CHECKPOINT, false));
        configuration.setBulkFlushSize(
                PropertiesUtil.getLong(properties, MongoOptions.BULK_FLUSH_SIZE, 1000L));
        configuration.setBulkFlushTime(
                PropertiesUtil.getLong(properties, MongoOptions.BULK_FLUSH_TIME, 30_000L));
        return configuration;
    }
}
