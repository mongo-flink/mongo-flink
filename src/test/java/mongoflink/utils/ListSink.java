package mongoflink.utils;

import com.google.common.collect.Lists;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.List;

/**
 * Sink that collects elements into a collection.
 **/
public class ListSink<IN> implements SinkFunction<IN> {

    private static List<Object> elements = Lists.newArrayList();

    public static <IN> List<IN> getElementsSet() {
        return (List<IN>) elements;
    }

    public static void clearElementsSet() {
        elements.clear();
    }

    @Override
    public void invoke(IN value, Context context) throws Exception {
        elements.add(value);
    }

}
