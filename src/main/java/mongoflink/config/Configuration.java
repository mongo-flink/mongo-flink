package mongoflink.config;

/**
 * Configuration for {@link mongoflink.sink.MongoSink}.
 **/
public class Configuration {

    private boolean isTransactional;

    private boolean isFlushOnCheckpoint;

    private long bulkFlushSize;

    private long bulkFlushTime;

    public boolean isTransactional() {
        return isTransactional;
    }

    public void setTransactional(boolean transactional) {
        isTransactional = transactional;
    }

    public boolean isFlushOnCheckpoint() {
        return isFlushOnCheckpoint;
    }

    public void setFlushOnCheckpoint(boolean flushOnCheckpoint) {
        isFlushOnCheckpoint = flushOnCheckpoint;
    }

    public long getBulkFlushSize() {
        return bulkFlushSize;
    }

    public void setBulkFlushSize(long bulkFlushSize) {
        this.bulkFlushSize = bulkFlushSize;
    }

    public long getBulkFlushTime() {
        return bulkFlushTime;
    }

    public void setBulkFlushTime(long bulkFlushTime) {
        this.bulkFlushTime = bulkFlushTime;
    }
}
