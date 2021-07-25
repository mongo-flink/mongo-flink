package mongoflink.source;

/**
 * The mutable version of mongo split.
 **/
public class MongoSplitState extends MongoSplit {

    private long currentOffset;

    MongoSplitState(MongoSplit mongoSplit) {
        super(mongoSplit.splitId(), mongoSplit.getQuery(), mongoSplit.getStartOffset());
        this.currentOffset = mongoSplit.getStartOffset();
    }

    public long getCurrentOffset() {
        return currentOffset;
    }

    public void increaseOffset(long n) {
        currentOffset += n;
    }
}
