package mongoflink.sink;

import mongoflink.EmbeddedMongoTestBase;
import mongoflink.serde.DocumentSerializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.bson.Document;

import java.util.Random;

/**
 * Base class for tests for MongoSink.
 **/

class MongoSinkTestBase extends EmbeddedMongoTestBase {

    protected static String DATABASE_NAME = "bulkwrite";
    protected static String COLLECTION = "transactional-4_0";

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
