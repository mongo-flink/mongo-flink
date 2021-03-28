package mongoflink.config;

/**
 * Config options for {@link mongoflink.sink.MongoSink}.
 **/
public class MongoOptions {

    public static final String TRANSACTION_ENABLED = "transaction.enable";

    public static final String BULK_FLUSH_ON_CHECKPOINT = "bulk.flush.on-checkpoint.enable";

    public static final String BULK_FLUSH_SIZE = "bulk.max.documents";

    public static final String BULK_FLUSH_TIME = "bulk.max.time";

}
